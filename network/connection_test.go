package network

import (
	"encoding/binary"
	"io"
	"testing"
	"time"
)

func TestNewConnection(t *testing.T) {
	portA, portB := getConnectedPorts()
	connA := NewConnection(portA, ConnectionOpts{})
	connB := NewConnection(portB, ConnectionOpts{})

	connA.Outgoing() <- &Message{Content: "msg.a1"}
	connB.Outgoing() <- &Message{Content: "msg.b1"}
	connA.Outgoing() <- &Message{Content: "msg.a2"}
	connB.Outgoing() <- &Message{Content: "msg.b2"}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a1" {
		t.Errorf("Expected msg.a1, got %v", msg)
		return
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a2" {
		t.Errorf("Expected msg.a2, got %v", msg)
		return
	}

	if msg := <-connA.Incoming(); msg == nil || msg.Content != "msg.b1" {
		t.Errorf("Expected msg.b1, got %v", msg)
		return
	}

	if msg := <-connA.Incoming(); msg == nil || msg.Content != "msg.b2" {
		t.Errorf("Expected msg.b2, got %v", msg)
		return
	}
}

func TestDroppedPackets(t *testing.T) {
	portA, portB := getConnectedPorts()
	connA := NewConnection(portA, ConnectionOpts{})
	connB := NewConnection(portB, ConnectionOpts{IncomingBuffer: 10})

	connA.SetDropOutgoingNth(2)

	connA.Outgoing() <- &Message{Content: "msg.a1"}
	connA.Outgoing() <- &Message{Content: "msg.a2"}
	connA.Outgoing() <- &Message{Content: "msg.a3"}
	connA.Outgoing() <- &Message{Content: "msg.a4"}
	connA.Outgoing() <- &Message{Content: "msg.a5"}
	connA.Outgoing() <- &Message{Content: "msg.a6"}
	connA.Outgoing() <- &Message{Content: "msg.a7"}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a1" {
		t.Errorf("Expected msg.a1, got %v", msg)
		return
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a3" {
		t.Errorf("Expected msg.a3, got %v", msg)
		return
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a5" {
		t.Errorf("Expected msg.a5, got %v", msg)
		return
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a7" {
		t.Errorf("Expected msg.a7, got %v", msg)
		return
	}
}
func TestIncomingDelay(t *testing.T) {
	portA, portB := getConnectedPorts()
	connA := NewConnection(portA, ConnectionOpts{OutgoingBuffer: 10})
	connB := NewConnection(portB, ConnectionOpts{IncomingBuffer: 10})

	connB.SetGlobalIncomingDelay(100 * time.Millisecond)

	sendingStart := time.Now()
	connA.Outgoing() <- &Message{Content: "msg.a1"}
	connA.Outgoing() <- &Message{Content: "msg.a2"}
	connA.Outgoing() <- &Message{Content: "msg.a3"}
	connA.Outgoing() <- &Message{Content: "msg.a4"}
	connA.Outgoing() <- &Message{Content: "msg.a5"}
	connA.Outgoing() <- &Message{Content: "msg.a6"}
	connA.Outgoing() <- &Message{Content: "msg.a7"}
	sendingDuration := time.Since(sendingStart)

	if sendingDuration > 500*time.Millisecond {
		t.Errorf("Expected sendingDuration <= 500ms (unaffected by incoming delay), got %v", sendingDuration)
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a1" {
		t.Errorf("Expected msg.a1, got %v", msg)
		return
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a2" {
		t.Errorf("Expected msg.a2, got %v", msg)
		return
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a3" {
		t.Errorf("Expected msg.a3, got %v", msg)
		return
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a4" {
		t.Errorf("Expected msg.a4, got %v", msg)
		return
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a5" {
		t.Errorf("Expected msg.a5, got %v", msg)
		return
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a6" {
		t.Errorf("Expected msg.a6, got %v", msg)
		return
	}

	if msg := <-connB.Incoming(); msg == nil || msg.Content != "msg.a7" {
		t.Errorf("Expected msg.a7, got %v", msg)
		return
	}

	receivingDuration := time.Since(sendingStart)
	if receivingDuration < 700*time.Millisecond {
		t.Errorf("Expected receivingDuration >= 700ms (from incoming delay), got %v", receivingDuration)
	}
}

func getConnectedPorts() (*MessagePort, *MessagePort) {
	pipeReaderA, pipeWriterA := io.Pipe()
	pipeReaderB, pipeWriterB := io.Pipe()

	portA := NewMessagePort(pipeReaderA, pipeWriterB)
	portB := NewMessagePort(pipeReaderB, pipeWriterA)

	return portA, portB
}

type Message struct {
	Content string
}

type MessagePort struct {
	Incoming io.Reader
	Outgoing io.Writer
}

func NewMessagePort(incoming io.Reader, outgoing io.Writer) *MessagePort {
	return &MessagePort{Incoming: incoming, Outgoing: outgoing}
}

func (mp *MessagePort) Read() (msg *Message, err error) {
	lengthBuf := make([]byte, 8)
	_, err = io.ReadFull(mp.Incoming, lengthBuf)
	if err != nil {
		return nil, err
	}

	strLen := binary.LittleEndian.Uint64(lengthBuf)

	strBuf := make([]byte, strLen)
	_, err = io.ReadFull(mp.Incoming, strBuf)
	if err != nil {
		return nil, err
	}

	return &Message{Content: string(strBuf)}, nil
}

func (mp *MessagePort) Write(msg *Message) (err error) {
	msgBuf := make([]byte, 8+len(msg.Content))
	binary.LittleEndian.PutUint64(msgBuf, uint64(len(msg.Content)))
	copy(msgBuf[8:], msg.Content)

	_, err = mp.Outgoing.Write(msgBuf)
	return err
}
