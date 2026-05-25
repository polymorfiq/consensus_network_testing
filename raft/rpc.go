package raft

import (
	"encoding/binary"
	"io"
	"slices"
)

type RpcMessageType uint8

const (
	AppendEntries RpcMessageType = iota
	RequestVote
	VoteResponse
	AppendResponse
)

type Rpc interface {
	MessageType() RpcMessageType
	Encode() []byte
	Decode(r io.Reader) error
}

type RequestVoteRpc struct {
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func NewRequestVoteRpc() *RequestVoteRpc {
	return &RequestVoteRpc{}
}

func (rpc *RequestVoteRpc) MessageType() RpcMessageType {
	return RequestVote
}

func (rpc *RequestVoteRpc) Encode() []byte {
	encoded := make([]byte, 8+8+8)

	binary.LittleEndian.PutUint64(encoded[0:], rpc.Term)
	binary.LittleEndian.PutUint64(encoded[8:], rpc.LastLogIndex)
	binary.LittleEndian.PutUint64(encoded[16:], rpc.LastLogTerm)

	return encoded
}

func (rpc *RequestVoteRpc) Decode(r io.Reader) error {
	metadata := make([]byte, 8+8+8)
	_, err := io.ReadFull(r, metadata)
	if err != nil {
		return err
	}

	rpc.Term = binary.LittleEndian.Uint64(metadata[0:])
	rpc.LastLogIndex = binary.LittleEndian.Uint64(metadata[8:])
	rpc.LastLogTerm = binary.LittleEndian.Uint64(metadata[16:])
	return nil
}

type VoteRespRpc struct {
	Term        uint64
	VoteGranted bool
}

func NewVoteRespRpc() *VoteRespRpc {
	return &VoteRespRpc{}
}

func (rpc *VoteRespRpc) MessageType() RpcMessageType {
	return VoteResponse
}

func (rpc *VoteRespRpc) Encode() []byte {
	encoded := make([]byte, 8+1)

	binary.LittleEndian.PutUint64(encoded[0:], rpc.Term)
	if rpc.VoteGranted {
		encoded[8] = 1
	} else {
		encoded[8] = 0
	}

	return encoded
}

func (rpc *VoteRespRpc) Decode(r io.Reader) error {
	metadata := make([]byte, 8+1)
	_, err := io.ReadFull(r, metadata)
	if err != nil {
		return err
	}

	rpc.Term = binary.LittleEndian.Uint64(metadata[0:])
	rpc.VoteGranted = metadata[8] == 1
	return nil
}

type AppendEntriesRpc struct {
	Term         uint64
	PrevLogIndex int64
	PrevLogTerm  uint64
	LeaderCommit uint64
	LogEntries   []LogTermEntry
}

func NewAppendEntriesRpc() *AppendEntriesRpc {
	return &AppendEntriesRpc{}
}

func (rpc *AppendEntriesRpc) MessageType() RpcMessageType {
	return AppendEntries
}

func (rpc *AppendEntriesRpc) Encode() []byte {
	encoded := make([]byte, 8+8+8+8+8)

	binary.LittleEndian.PutUint64(encoded[0:], rpc.Term)
	binary.LittleEndian.PutUint64(encoded[8:], uint64(rpc.PrevLogIndex))
	binary.LittleEndian.PutUint64(encoded[16:], rpc.PrevLogTerm)
	binary.LittleEndian.PutUint64(encoded[24:], rpc.LeaderCommit)

	binary.LittleEndian.PutUint64(encoded[32:], uint64(len(rpc.LogEntries)))

	for _, entry := range rpc.LogEntries {
		entryTerm := make([]byte, 8)
		entryType := make([]byte, 8)

		binary.LittleEndian.PutUint64(entryTerm[:], entry.Term)
		binary.LittleEndian.PutUint64(entryType[:], uint64(entry.Entry.EntryType()))
		encodedEntry := entry.Entry.Encode()
		encoded = slices.Concat(encoded, entryTerm[:], entryType[:], encodedEntry)
	}

	return encoded
}

func (rpc *AppendEntriesRpc) Decode(r io.Reader) error {
	metadata := make([]byte, 8+8+8+8+8)
	_, err := io.ReadFull(r, metadata)
	if err != nil {
		return err
	}

	rpc.Term = binary.LittleEndian.Uint64(metadata[0:])
	rpc.PrevLogIndex = int64(binary.LittleEndian.Uint64(metadata[8:]))
	rpc.PrevLogTerm = binary.LittleEndian.Uint64(metadata[16:])
	rpc.LeaderCommit = binary.LittleEndian.Uint64(metadata[24:])
	logEntryLen := binary.LittleEndian.Uint64(metadata[32:])

	for range logEntryLen {
		entryMetadata := make([]byte, 16)
		_, err := io.ReadFull(r, entryMetadata[:])
		if err != nil {
			return err
		}

		entryTerm := binary.LittleEndian.Uint64(entryMetadata[:])
		entryType := LogEntryType(binary.LittleEndian.Uint64(entryMetadata[8:]))
		switch entryType {
		case SetLogEntryType:
			setEntry := SetLogEntry{}
			err := setEntry.Decode(r)
			if err != nil {
				return err
			}

			rpc.LogEntries = append(rpc.LogEntries, LogTermEntry{entryTerm, &setEntry})

		case DeleteLogEntryType:
			deleteEntry := DeleteLogEntry{}
			err := deleteEntry.Decode(r)
			if err != nil {
				return err
			}

			rpc.LogEntries = append(rpc.LogEntries, LogTermEntry{entryTerm, &deleteEntry})
		}
	}

	return nil
}

type AppendRespRpc struct {
	Term    uint64
	Success bool
}

func NewAppendRespRpc() *AppendRespRpc {
	return &AppendRespRpc{}
}

func (rpc *AppendRespRpc) MessageType() RpcMessageType {
	return AppendResponse
}

func (rpc *AppendRespRpc) Encode() []byte {
	encoded := make([]byte, 8+1)

	binary.LittleEndian.PutUint64(encoded[0:], rpc.Term)
	if rpc.Success {
		encoded[8] = 1
	} else {
		encoded[8] = 0
	}

	return encoded
}

func (rpc *AppendRespRpc) Decode(r io.Reader) error {
	metadata := make([]byte, 8+1)
	_, err := io.ReadFull(r, metadata)
	if err != nil {
		return err
	}

	rpc.Term = binary.LittleEndian.Uint64(metadata[:])
	rpc.Success = metadata[8] == 1
	return nil
}
