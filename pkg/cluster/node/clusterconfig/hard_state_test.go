package clusterconfig

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestDurableVoteSurvivesStorageReopen(t *testing.T) {
	dir := t.TempDir()
	s := NewPebbleShardLogStorage(dir, nil)
	require.NoError(t, s.Open())
	state, err := s.GetState()
	require.NoError(t, err)
	require.Equal(t, types.HardState{}, state.HardState)
	n := raft.NewNode(0, state, raft.NewOptions(raft.WithNodeId(1), raft.WithReplicas([]uint64{1, 2, 3}), raft.WithSaveHardState(s.SaveHardState)))
	vote := types.Event{Type: types.VoteReq, From: 2, Term: 7, Logs: []types.Log{{Term: 0}}}
	require.NoError(t, n.Step(vote))
	events := n.Ready()
	require.Len(t, events, 1)
	require.Equal(t, types.ReasonOk, events[0].Reason)
	require.NoError(t, s.Close())
	s = NewPebbleShardLogStorage(dir, nil)
	require.NoError(t, s.Open())
	defer func() { require.NoError(t, s.Close()) }()
	state, err = s.GetState()
	require.NoError(t, err)
	require.Equal(t, types.HardState{Term: 7, Vote: 2}, state.HardState)
	require.Zero(t, state.LastTerm, "elections without log appends must survive restart")
	n = raft.NewNode(0, state, raft.NewOptions(raft.WithNodeId(1), raft.WithReplicas([]uint64{1, 2, 3}), raft.WithSaveHardState(s.SaveHardState)))
	vote.From = 3
	require.NoError(t, n.Step(vote))
	events = n.Ready()
	require.Len(t, events, 1)
	require.Equal(t, types.ReasonError, events[0].Reason)
	vote.Term++
	require.NoError(t, n.Step(vote))
	events = n.Ready()
	require.Len(t, events, 1)
	require.Equal(t, types.ReasonOk, events[0].Reason)
}
