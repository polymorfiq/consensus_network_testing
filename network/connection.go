package network

import (
	"time"
)

type Connection[T any] struct {
	outgoing            chan T
	incoming            chan T
	readErrors          chan error
	writeErrors         chan error
	sentCounter         int
	receivedCounter     int
	dropIncomingNth     int
	dropOutgoingNth     int
	globalIncomingDelay time.Duration
	globalOutgoingDelay time.Duration
}

type ConnectionOpts struct {
	OutgoingBuffer    int
	IncomingBuffer    int
	ReadErrorsBuffer  int
	WriteErrorsBuffer int
}

type StructuredPort[T any] interface {
	Read() (T, error)
	Write(T) error
}

func NewConnection[T any](port StructuredPort[T], opts ConnectionOpts) *Connection[T] {
	outgoing := make(chan T, opts.OutgoingBuffer)
	incoming := make(chan T, opts.IncomingBuffer)
	readErrors := make(chan error, opts.ReadErrorsBuffer)
	writeErrors := make(chan error, opts.WriteErrorsBuffer)
	conn := &Connection[T]{
		outgoing:    outgoing,
		incoming:    incoming,
		readErrors:  readErrors,
		writeErrors: writeErrors,
	}

	go func() {
		for {
			resp, err := port.Read()
			conn.receivedCounter++

			if conn.dropIncomingNth > 0 && (conn.receivedCounter%conn.dropIncomingNth) == 0 {
				continue
			}

			if conn.globalIncomingDelay > 0 {
				time.Sleep(conn.globalIncomingDelay)
			}

			if err != nil {
				readErrors <- err
			} else {
				incoming <- resp
			}
		}
	}()

	go func() {
		for {
			sentData := <-outgoing
			conn.sentCounter++

			if conn.dropOutgoingNth > 0 && (conn.sentCounter%conn.dropOutgoingNth) == 0 {
				continue
			}

			if conn.globalOutgoingDelay > 0 {
				time.Sleep(conn.globalOutgoingDelay)
			}

			err := port.Write(sentData)
			if err != nil {
				writeErrors <- err
			}
		}
	}()

	return conn
}

func (c *Connection[T]) SetDropIncomingNth(nth int) {
	c.dropIncomingNth = nth
}

func (c *Connection[T]) SetDropOutgoingNth(nth int) {
	c.dropOutgoingNth = nth
}

func (c *Connection[T]) SetGlobalOutgoingDelay(delay time.Duration) {
	c.globalOutgoingDelay = delay
}

func (c *Connection[T]) SetGlobalIncomingDelay(delay time.Duration) {
	c.globalIncomingDelay = delay
}

func (c *Connection[T]) Outgoing() chan<- T {
	return c.outgoing
}

func (c *Connection[T]) Incoming() <-chan T {
	return c.incoming
}

func (c *Connection[T]) ReadErrors() <-chan error {
	return c.readErrors
}

func (c *Connection[T]) WriteErrors() <-chan error {
	return c.writeErrors
}
