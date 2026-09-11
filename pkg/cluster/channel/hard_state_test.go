package channel

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func TestDurableVoteSurvivesStorageReopen(t *testing.T) {
	dir := t.TempDir()
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(2)))
	require.NoError(t, db.Open())
	s := newStorage(db, nil)
	state, err := s.GetState("channel", 2)
	require.NoError(t, err)
	require.Equal(t, types.HardState{}, state.HardState)
	n := raft.NewNode(0, state, raft.NewOptions(raft.WithNodeId(1), raft.WithReplicas([]uint64{1, 2, 3}), raft.WithSaveHardState(func(h types.HardState) error { return db.SaveRaftHardState(wkutil.ChannelToKey("channel", 2), h) })))
	vote := types.Event{Type: types.VoteReq, From: 2, Term: 7, Logs: []types.Log{{Term: 0}}}
	require.NoError(t, n.Step(vote))
	events := n.Ready()
	require.Len(t, events, 1)
	require.Equal(t, types.ReasonOk, events[0].Reason)
	require.NoError(t, db.Close())
	db = wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(2)))
	require.NoError(t, db.Open())
	s = newStorage(db, nil)
	defer func() { require.NoError(t, db.Close()) }()
	state, err = s.GetState("channel", 2)
	require.NoError(t, err)
	require.Equal(t, types.HardState{Term: 7, Vote: 2}, state.HardState)
	require.Zero(t, state.LastTerm, "elections without log appends must survive restart")
	n = raft.NewNode(0, state, raft.NewOptions(raft.WithNodeId(1), raft.WithReplicas([]uint64{1, 2, 3}), raft.WithSaveHardState(func(h types.HardState) error { return db.SaveRaftHardState(wkutil.ChannelToKey("channel", 2), h) })))
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

func TestCreateChannelRestoresDurableVote(t *testing.T) {
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(2)))
	require.NoError(t, db.Open())
	defer db.Close()
	s := &Server{opts: NewOptions(WithNodeId(1))}
	s.storage = newStorage(db, s)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "channel", ChannelType: 2}
	ch, err := createChannel(cfg, s, nil)
	require.NoError(t, err)
	membership := types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1, 2, 3}, Role: types.RoleFollower, Term: 7}}
	require.NoError(t, ch.Step(membership))
	vote := types.Event{Type: types.VoteReq, From: 2, Term: 7, Logs: []types.Log{{}}}
	require.NoError(t, ch.Step(vote))
	require.NotEmpty(t, ch.Ready())
	// Destroying/reactivating a channel uses the same production constructor as restart.
	ch, err = createChannel(cfg, s, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Step(membership))
	vote.From = 3
	require.NoError(t, ch.Step(vote))
	events := ch.Ready()
	require.Len(t, events, 1)
	require.Equal(t, types.ReasonError, events[0].Reason)
}
