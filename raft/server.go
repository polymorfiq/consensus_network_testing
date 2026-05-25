package raft

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"network-simulation/network"
	"sync"
	"time"

	"github.com/acomagu/bufpipe"
)

type Server struct {
	Id          string
	State       ConsensusState
	CurrentTerm uint64
	Log         []LogTermEntry
	CommitIndex uint64
	NextIndex   map[network.RouterAddress]uint64
	SentIndex   map[network.RouterAddress]uint64
	MatchIndex  map[network.RouterAddress]uint64
	LeaderId    *network.RouterAddress

	router    *network.Router[Rpc]
	votedFor  *network.RouterAddress
	voteCount uint64
	stateLock sync.Mutex
	logRW     sync.RWMutex
}

func NewServer(id string) *Server {
	server := Server{
		Id:         id,
		State:      Follower,
		router:     network.NewRouter[Rpc](),
		NextIndex:  make(map[network.RouterAddress]uint64),
		SentIndex:  make(map[network.RouterAddress]uint64),
		MatchIndex: make(map[network.RouterAddress]uint64),
	}

	go server.processRpcs()

	return &server
}

func (server *Server) Append(entry LogEntry) (bool, *network.RouterAddress, error) {
	server.stateLock.Lock()
	server.logRW.Lock()
	defer server.stateLock.Unlock()
	defer server.logRW.Unlock()

	if server.State != Leader && server.LeaderId != nil {
		return false, server.LeaderId, nil
	} else if server.State != Leader {
		return false, nil, errors.New("server is not leader and does not know leader")
	}

	server.Log = append(server.Log, LogTermEntry{Term: server.CurrentTerm, Entry: entry})

	return true, nil, nil
}

func (server *Server) Retrieve(key string) ([]byte, bool, *network.RouterAddress, error) {
	server.stateLock.Lock()
	server.logRW.Lock()
	defer server.stateLock.Unlock()
	defer server.logRW.Unlock()

	if server.State != Leader && server.LeaderId != nil {
		return nil, false, server.LeaderId, nil
	} else if server.State != Leader {
		return nil, false, nil, errors.New("server is not leader and does not know leader")
	}

	for i := len(server.Log); i > 0; i-- {
		entry := server.Log[i-1]

		switch entry.Entry.(type) {
		case *SetLogEntry:
			setKey := entry.Entry.(*SetLogEntry)
			if setKey.Key == key {
				return setKey.Value, true, nil, nil
			}

		case *DeleteLogEntry:
			if entry.Entry.(*DeleteLogEntry).Key == key {
				return nil, false, nil, nil
			}
		}
	}

	return nil, false, nil, nil
}

func (server *Server) performLeadership() {
	for addr := range server.router.Addresses() {
		go func() {
			server.replicateTo(addr)
		}()
	}
}

func (server *Server) replicateTo(addr network.RouterAddress) {
	server.logRW.RLock()
	defer server.logRW.RUnlock()

	server.stateLock.Lock()
	nextIdx, ok := server.NextIndex[addr]
	server.stateLock.Unlock()

	if !ok {
		nextIdx = uint64(len(server.Log))
	}

	appendReq := NewAppendEntriesRpc()
	appendReq.Term = server.CurrentTerm
	appendReq.PrevLogIndex = int64(nextIdx) - 1
	appendReq.LeaderCommit = server.CommitIndex

	if appendReq.PrevLogIndex >= 0 {
		appendReq.PrevLogTerm = server.Log[appendReq.PrevLogIndex].Term
	}

	for _, entry := range server.Log[nextIdx:] {
		appendReq.LogEntries = append(appendReq.LogEntries, entry)
	}

	if len(appendReq.LogEntries) > 0 {
		fmt.Printf("%s: Replicating (%d of %d) %d to %s\n", server.Id, len(server.Log), nextIdx, len(appendReq.LogEntries), addr)
	}

	msg := network.NewRouterMessage[Rpc]()
	msg.RouterDeliveryType = network.BroadcastTargets
	msg.Targets = []network.RouterAddress{addr}
	msg.Message = appendReq
	server.router.Send(msg)

	server.stateLock.Lock()
	server.SentIndex[addr] = uint64(len(server.Log))
	defer server.stateLock.Unlock()
}

func (server *Server) processRpcs() {
	for {
		var heartbeat <-chan time.Time
		if server.State == Leader {
			heartbeat = time.After(time.Millisecond * time.Duration(50))
		} else {
			electionTimeoutJiggle := rand.IntN(150)
			heartbeat = time.After(time.Millisecond * time.Duration(150+electionTimeoutJiggle))
		}

		select {
		case receipt, ok := <-server.router.Incoming():
			if !ok {
				fmt.Println("Server receipt channel closed")
				return
			}

			switch receipt.Message.MessageType() {
			case AppendResponse:
				appendResp := receipt.Message.(*AppendRespRpc)

				server.stateLock.Lock()
				sentIdx, didSendIdx := server.SentIndex[receipt.FromAddress]
				if didSendIdx && server.NextIndex[receipt.FromAddress] != sentIdx {
					fmt.Printf("%s: Received AppendResponse from %s - %v\n", server.Id, receipt.FromAddress, appendResp.Success)
				}
				server.stateLock.Unlock()

				if appendResp.Term > server.CurrentTerm {
					server.becomeFollower(&receipt.FromAddress, appendResp.Term)
					continue
				}

				if server.State == Leader && appendResp.Success {
					server.stateLock.Lock()
					server.MatchIndex[receipt.FromAddress] = server.SentIndex[receipt.FromAddress]
					server.NextIndex[receipt.FromAddress] = server.SentIndex[receipt.FromAddress]
					server.stateLock.Unlock()
				} else if server.State == Leader && !appendResp.Success {
					server.stateLock.Lock()
					server.NextIndex[receipt.FromAddress]--
					server.stateLock.Unlock()
				}

			case VoteResponse:
				voteResp := receipt.Message.(*VoteRespRpc)

				if voteResp.Term > server.CurrentTerm {
					server.CurrentTerm = voteResp.Term
					continue
				}

				if voteResp.VoteGranted && server.State == Candidate {
					fmt.Printf("%s: Received vote from %s\n", server.Id, receipt.FromAddress)

					server.voteCount += 1

					majorityConns := math.Floor(float64(server.router.NumConnections())/2.0) + 1
					if server.voteCount >= uint64(majorityConns) {
						server.becomeLeader()
						server.performLeadership()
					}
				}

			case AppendEntries:
				appendEntries := receipt.Message.(*AppendEntriesRpc)

				appendResp := NewAppendRespRpc()
				appendResp.Term = server.CurrentTerm
				appendResp.Success = true

				if server.State == Leader && appendEntries.Term > server.CurrentTerm {
					server.becomeFollower(&receipt.FromAddress, appendEntries.Term)
				} else if server.State == Leader {
					fmt.Printf("%s: Append rejected from %s: (Competitor Leader)\n", server.Id, receipt.FromAddress)
					appendResp.Success = false
				} else if appendEntries.Term >= server.CurrentTerm {
					server.becomeFollower(&receipt.FromAddress, appendEntries.Term)
				} else if server.State == Candidate {
					fmt.Printf("%s: Append rejected from %s: (Still Candidate)\n", server.Id, receipt.FromAddress)
					appendResp.Success = false
				}

				logLen := uint64(len(server.Log))
				if appendEntries.PrevLogIndex < 0 {
					appendResp.Success = true
					server.Log = nil
				} else if appendEntries.PrevLogIndex < int64(logLen) {
					prevLogEntry := server.Log[appendEntries.PrevLogIndex]

					if prevLogEntry.Term != appendEntries.PrevLogTerm {
						fmt.Printf("%s: Append rejected from %s: (Prev Log Term)\n", server.Id, receipt.FromAddress)
						appendResp.Success = false
					}
				} else {
					fmt.Printf("%s: Append rejected from %s: (Missing indices - %d vs %d)\n", server.Id, receipt.FromAddress, appendEntries.PrevLogIndex, len(server.Log))
					appendResp.Success = false
				}

				if appendResp.Success {
					server.logRW.Lock()
					server.Log = server.Log[:appendEntries.PrevLogIndex+1]
					for _, entry := range appendEntries.LogEntries {
						server.Log = append(server.Log, entry)
					}
					server.logRW.Unlock()

					if len(appendEntries.LogEntries) > 0 {
						fmt.Printf("%s: Stored entries from %s: %d\n", server.Id, receipt.FromAddress, len(appendEntries.LogEntries))
					}
				}

				msg := network.NewRouterMessage[Rpc]()
				msg.RouterDeliveryType = network.BroadcastTargets
				msg.Targets = []network.RouterAddress{receipt.FromAddress}
				msg.Message = appendResp

				if len(appendEntries.LogEntries) > 0 {
					err := server.router.Send(msg)
					if err != nil {
						fmt.Printf("Failed to send AppendResponse to %s: %v\n", receipt.FromAddress, err)
						continue
					}
				}

			case RequestVote:
				voteReq := (receipt.Message).(*RequestVoteRpc)

				voteResp := NewVoteRespRpc()
				voteResp.Term = server.CurrentTerm
				voteResp.VoteGranted = true

				if voteReq.Term < server.CurrentTerm {
					fmt.Printf("%s: Vote Rejected for %s: (Outdated Term)\n", server.Id, receipt.FromAddress)
					voteResp.VoteGranted = false
				} else if server.State == Candidate && voteReq.Term <= server.CurrentTerm {
					voteResp.VoteGranted = false
					fmt.Printf("%s: Vote Rejected for %s: (Competitor Candidate)\n", server.Id, receipt.FromAddress)
				}

				if server.votedFor != nil && (*server.votedFor) != receipt.FromAddress {
					voteResp.VoteGranted = false
					fmt.Printf("%s: Vote Rejected for %s: (Already Voted)\n", server.Id, receipt.FromAddress)
				}

				var lastEntry LogTermEntry
				if len(server.Log) > 0 {
					lastEntry = server.Log[len(server.Log)-1]
				}

				if voteReq.LastLogIndex < uint64(len(server.Log)) {
					fmt.Printf("%s: Vote Rejected for %s: (LastLogIndex)\n", server.Id, receipt.FromAddress)
					voteResp.VoteGranted = false
				} else if len(server.Log) > 0 && voteReq.LastLogTerm < lastEntry.Term {
					fmt.Printf("%s: Vote Rejected for %s: (LastLogTerm)\n", server.Id, receipt.FromAddress)
					voteResp.VoteGranted = false
				}

				msg := network.NewRouterMessage[Rpc]()
				msg.RouterDeliveryType = network.BroadcastTargets
				msg.Targets = []network.RouterAddress{receipt.FromAddress}
				msg.Message = voteResp
				server.router.Send(msg)

				if voteResp.VoteGranted {
					votedFor := new(network.RouterAddress)
					*votedFor = receipt.FromAddress
					server.votedFor = votedFor

					fmt.Printf("%s: Voted for %s\n", server.Id, receipt.FromAddress)
					server.becomeFollower(&receipt.FromAddress, voteReq.Term)
				}
			}

		case <-heartbeat:
			if server.State == Leader {
				server.performLeadership()
			} else {
				server.electNewLeader()
			}
		}
	}
}

func (server *Server) becomeFollower(leaderId *network.RouterAddress, term uint64) {
	server.stateLock.Lock()
	defer server.stateLock.Unlock()
	server.CurrentTerm = term
	server.LeaderId = leaderId
	server.votedFor = nil

	if server.State != Follower {
		fmt.Printf("%s: becomeFollower\n", server.Id)

		server.State = Follower
	}
}

func (server *Server) becomeLeader() {
	server.stateLock.Lock()
	defer server.stateLock.Unlock()
	fmt.Printf("%s: becomeLeader\n", server.Id)

	server.LeaderId = nil
	server.State = Leader
	server.voteCount = 0
	server.votedFor = nil

	for serverId := range server.router.Addresses() {
		server.NextIndex[serverId] = uint64(len(server.Log))

		_, matchIdSeen := server.MatchIndex[serverId]
		if !matchIdSeen {
			server.MatchIndex[serverId] = 0
		}
	}
}

func (server *Server) electNewLeader() {
	server.stateLock.Lock()
	defer server.stateLock.Unlock()
	fmt.Printf("%s: electNewLeader: %d\n", server.Id, server.CurrentTerm)

	server.State = Candidate
	server.CurrentTerm += 1
	server.votedFor = nil

	reqVote := NewRequestVoteRpc()
	reqVote.Term = server.CurrentTerm
	reqVote.LastLogIndex = uint64(len(server.Log))
	if len(server.Log) > 0 {
		lastEntry := server.Log[len(server.Log)-1]
		reqVote.LastLogTerm = lastEntry.Term
	}

	msg := network.NewRouterMessage[Rpc]()
	msg.RouterDeliveryType = network.BroadcastAll
	msg.Message = reqVote

	server.router.Send(msg)
	server.voteCount = 1
}

func (server *Server) Connect(neighbor *Server) {
	portA, portB := getConnectedRpcPorts()
	connA := network.NewConnection(portA, network.ConnectionOpts{OutgoingBuffer: 10, IncomingBuffer: 10})
	connB := network.NewConnection(portB, network.ConnectionOpts{OutgoingBuffer: 10, IncomingBuffer: 10})

	fmt.Printf("%s: Connecting to %s\n", server.Id, neighbor.Id)
	server.router.Add(network.RouterAddress(neighbor.Id), *connA)
	neighbor.router.Add(network.RouterAddress(server.Id), *connB)
}

func getConnectedRpcPorts() (*RpcPort, *RpcPort) {
	pipeReaderA, pipeWriterA := bufpipe.New(nil)
	pipeReaderB, pipeWriterB := bufpipe.New(nil)

	portA := NewRpcPort(pipeReaderA, pipeWriterB)
	portB := NewRpcPort(pipeReaderB, pipeWriterA)

	return portA, portB
}
