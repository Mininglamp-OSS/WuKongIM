package raft

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func voteRequest(from uint64, term uint32) types.Event {
	return types.Event{Type: types.VoteReq, From: from, Term: term, Logs: []types.Log{{Term: 1}}}
}

func TestVoteSurvivesSameTermTransitions(t *testing.T) {
	transitions := map[string]func(*Node){
		"follower":         func(n *Node) { n.BecomeFollower(3, 2) },
		"leader":           func(n *Node) { n.BecomeLeader(3) },
		"learner and back": func(n *Node) { n.BecomeLearner(3, 2); n.BecomeFollower(3, 2) },
		"first heartbeat":  func(n *Node) { require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 2, Term: 3})) },
		"configuration": func(n *Node) {
			cfg := n.cfg.Clone()
			cfg.Version++
			cfg.Leader = 2
			require.NoError(t, n.switchConfig(cfg))
		},
	}
	for name, transition := range transitions {
		t.Run(name, func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2, 3})
			require.NoError(t, n.Step(voteRequest(2, 3)))
			first, ok := findEvent(n.Ready(), types.VoteResp)
			require.True(t, ok)
			require.Equal(t, types.ReasonOk, first.Reason)
			transition(n)
			retry := voteRequest(3, 3)
			retry.ConfigVersion = n.cfg.Version
			require.NoError(t, n.Step(retry))
			second, ok := findEvent(n.Ready(), types.VoteResp)
			require.True(t, ok)
			require.Equal(t, types.ReasonError, second.Reason)
			retry.Term = 4
			require.NoError(t, n.Step(retry))
			next, ok := findEvent(n.Ready(), types.VoteResp)
			require.True(t, ok)
			require.Equal(t, types.ReasonOk, next.Reason)
		})
	}
}

func TestHardStateBlocksEventsUntilDurable(t *testing.T) {
	for _, campaign := range []bool{false, true} {
		t.Run(map[bool]string{false: "grant", true: "campaign"}[campaign], func(t *testing.T) {
			var saved types.HardState
			fail := true
			n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithSaveHardState(func(s types.HardState) error {
				if fail {
					return errors.New("disk unavailable")
				}
				saved = s
				return nil
			})))
			if campaign {
				n.campaign()
			} else {
				require.NoError(t, n.Step(voteRequest(2, 3)))
			}
			require.Empty(t, n.Ready(), "no election message may escape a failed write")
			require.True(t, n.HasReady(), "failed writes must remain retryable")
			require.Zero(t, saved.Term)
			fail = false
			events := n.Ready()
			require.NotEmpty(t, events)
			require.Equal(t, n.cfg.Term, saved.Term)
			require.Equal(t, n.voteFor, saved.Vote)
			restarted := NewNode(0, types.RaftState{HardState: saved}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3})))
			require.NoError(t, restarted.Step(voteRequest(3, saved.Term)))
			resp, ok := findEvent(restarted.Ready(), types.VoteResp)
			require.True(t, ok)
			require.Equal(t, types.ReasonError, resp.Reason)
		})
	}
}

func TestHardStateRestoresTermIndependentlyOfLogTerm(t *testing.T) {
	n := NewNode(1, types.RaftState{LastLogIndex: 2, LastTerm: 3, AppliedIndex: 2, HardState: types.HardState{Term: 9, Vote: 2}}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3})))
	require.Equal(t, uint32(9), n.LastTerm())
	require.Equal(t, uint32(3), n.LastLogTerm())
	n.BecomeFollower(8, 3)
	require.Equal(t, uint32(9), n.LastTerm())
	require.Equal(t, uint64(2), n.voteFor)
	require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 3, Term: 8}))
	require.Equal(t, uint64(2), n.voteFor)
	n.BecomeFollower(10, 3)
	require.Zero(t, n.voteFor)
}

func TestLeaderReadWaitsForDurableTerm(t *testing.T) {
	fail := true
	n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithSaveHardState(func(types.HardState) error {
		if fail {
			return errors.New("disk unavailable")
		}
		return nil
	})))
	require.False(t, n.LeaderReadReady())
	require.Empty(t, n.Ready())
	require.False(t, n.LeaderReadReady())
	fail = false
	n.Ready()
	require.True(t, n.LeaderReadReady())
}
