package raftgroup

import (
	"context"
	"errors"
)

// ReadState is a scalar snapshot taken on the Raft event loop. It is a local
// role/configuration check, not a quorum ReadIndex or a leader lease.
type ReadState struct {
	Exists         bool
	LeaderID       uint64
	Term           uint32
	ConfigVersion  uint64
	Ready          bool
	CommittedIndex uint64
	AppliedIndex   uint64
}

type readStateRequest struct {
	key    string
	result chan ReadState
}

// ReadLeaderState avoids racing Step/Tick when validating an authoritative read.
func (rg *RaftGroup) ReadLeaderState(ctx context.Context, key string) (ReadState, error) {
	if err := ctx.Err(); err != nil {
		return ReadState{}, err
	}
	req := readStateRequest{key: key, result: make(chan ReadState, 1)}
	select {
	case rg.readStateC <- req:
	case <-ctx.Done():
		return ReadState{}, ctx.Err()
	case <-rg.stopper.ShouldStop():
		return ReadState{}, errors.New("raft group stopped")
	}
	select {
	case state := <-req.result:
		return state, ctx.Err()
	case <-ctx.Done():
		return ReadState{}, ctx.Err()
	case <-rg.stopper.ShouldStop():
		return ReadState{}, errors.New("raft group stopped")
	}
}

func (rg *RaftGroup) handleReadState(req readStateRequest) {
	var state ReadState
	if r := rg.GetRaft(req.key); r != nil {
		cfg := r.Config()
		state = ReadState{Exists: true, LeaderID: cfg.Leader, Term: cfg.Term,
			ConfigVersion: cfg.Version, CommittedIndex: r.CommittedIndex(), AppliedIndex: r.AppliedIndex()}
		// Unknown implementations cannot certify that writes have not been fenced.
		if reader, ok := r.(interface{ LeaderReadReady() bool }); ok {
			state.Ready = reader.LeaderReadReady()
		}
	}
	req.result <- state
}
