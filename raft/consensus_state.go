package raft

type ConsensusState int

const (
	Follower ConsensusState = iota
	Leader
	Candidate
)
