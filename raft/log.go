package raft

import (
	"encoding/binary"
	"io"
	"slices"
)

type Log struct {
	entries []LogEntry
}

type LogEntryType uint64
type LogEntry interface {
	EntryType() LogEntryType
	Encode() []byte
	Decode(io.Reader) error
}

type LogTermEntry struct {
	Term  uint64
	Entry LogEntry
}

const (
	SetLogEntryType LogEntryType = iota
	DeleteLogEntryType
)

type SetLogEntry struct {
	Key   string
	Value []byte
}

func (e *SetLogEntry) EntryType() LogEntryType {
	return SetLogEntryType
}

func (e *SetLogEntry) Encode() []byte {
	var metadata [16]byte

	strBytes := []byte(e.Key)
	binary.LittleEndian.PutUint64(metadata[0:], uint64(len(strBytes)))
	binary.LittleEndian.PutUint64(metadata[8:], uint64(len(e.Value)))

	return slices.Concat(metadata[:], strBytes, e.Value)
}

func (e *SetLogEntry) Decode(r io.Reader) error {
	var metadata [16]byte
	if _, err := io.ReadFull(r, metadata[:]); err != nil {
		return err
	}

	keyBytes := make([]byte, binary.LittleEndian.Uint64(metadata[:]))
	if _, err := io.ReadFull(r, keyBytes[:]); err != nil {
		return err
	}

	valueBytes := make([]byte, binary.LittleEndian.Uint64(metadata[8:]))
	if _, err := io.ReadFull(r, valueBytes[:]); err != nil {
		return err
	}

	e.Key = string(keyBytes)
	e.Value = valueBytes
	return nil
}

type DeleteLogEntry struct {
	Key string
}

func (e *DeleteLogEntry) EntryType() LogEntryType {
	return DeleteLogEntryType
}

func (e *DeleteLogEntry) Encode() []byte {
	var metadata [8]byte

	strBytes := []byte(e.Key)
	binary.LittleEndian.PutUint64(metadata[:], uint64(len(strBytes)))

	return slices.Concat(metadata[:], strBytes)
}

func (e *DeleteLogEntry) Decode(r io.Reader) error {
	var metadata [8]byte
	if _, err := io.ReadFull(r, metadata[:]); err != nil {
		return err
	}

	keyBytes := make([]byte, binary.LittleEndian.Uint64(metadata[:]))
	if _, err := io.ReadFull(r, keyBytes[:]); err != nil {
		return err
	}

	e.Key = string(keyBytes)
	return nil
}
