package raft

import (
	"fmt"
	"io"
	"slices"
)

type RpcPort struct {
	Incoming io.Reader
	Outgoing io.Writer
}

func NewRpcPort(incoming io.Reader, outgoing io.Writer) *RpcPort {
	return &RpcPort{Incoming: incoming, Outgoing: outgoing}
}

func (port *RpcPort) Read() (rpc Rpc, err error) {
	var rpcMessageTypeBuf [1]byte
	_, err = io.ReadFull(port.Incoming, rpcMessageTypeBuf[:])
	if err != nil {
		return nil, err
	}

	switch RpcMessageType(rpcMessageTypeBuf[0]) {
	case AppendEntries:
		appendEntries := NewAppendEntriesRpc()
		if err = appendEntries.Decode(port.Incoming); err != nil {
			return
		}

		return appendEntries, nil

	case RequestVote:
		voteReq := NewRequestVoteRpc()
		if err = voteReq.Decode(port.Incoming); err != nil {
			return
		}

		return voteReq, nil

	case VoteResponse:
		voteResp := NewVoteRespRpc()
		if err = voteResp.Decode(port.Incoming); err != nil {
			return
		}

		return voteResp, nil

	case AppendResponse:
		appendResp := NewAppendRespRpc()
		if err = appendResp.Decode(port.Incoming); err != nil {
			return
		}

		return appendResp, nil

	default:
		err = fmt.Errorf("unknown rpc message type: %v", RpcMessageType(rpcMessageTypeBuf[0]))
		return
	}
}

func (port *RpcPort) Write(rpc Rpc) (err error) {
	var rpcMessageTypeBuf [1]byte
	rpcMessageTypeBuf[0] = byte(rpc.MessageType())

	rpcBuf := slices.Concat(rpcMessageTypeBuf[:], rpc.Encode())
	if _, err = port.Outgoing.Write(rpcBuf); err != nil {
		return
	}

	return nil
}
