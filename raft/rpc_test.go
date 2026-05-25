package raft

import (
	"testing"

	"github.com/acomagu/bufpipe"
)

func TestAppendEntries(t *testing.T) {
	r, w := bufpipe.New(nil)

	appendReq := AppendEntriesRpc{
		Term:         1,
		PrevLogIndex: 2,
		PrevLogTerm:  3,
		LogEntries:   []LogTermEntry{{123, &SetLogEntry{Key: "ABC", Value: []byte("DEF")}}},
	}

	if _, err := w.Write(appendReq.Encode()); err != nil {
		t.Fatal(err)
	}

	readReq := AppendEntriesRpc{}
	if err := readReq.Decode(r); err != nil {
		t.Fatal(err)
	}

	if readReq.Term != 1 {
		t.Errorf("Expected term 1, but got %d", readReq.Term)
	}

	if readReq.PrevLogIndex != 2 {
		t.Errorf("Expected prev log index 2, but got %d", readReq.PrevLogIndex)
	}

	if readReq.PrevLogTerm != 3 {
		t.Errorf("Expected prev log term 3, but got %d", readReq.PrevLogTerm)
	}

	if readReq.LogEntries[0].Term != 123 {
		t.Errorf("Expected log entries term 123, but got %d", readReq.LogEntries[0].Term)
	}

	if readReq.LogEntries[0].Entry.(*SetLogEntry).Key != "ABC" {
		t.Errorf("Expected log entries key ABC, but got %v", readReq.LogEntries[0].Entry)
	}

	if string(readReq.LogEntries[0].Entry.(*SetLogEntry).Value) != "DEF" {
		t.Errorf("Expected log entries value DEF, but got %v", string(readReq.LogEntries[0].Entry.(*SetLogEntry).Value))
	}
}
