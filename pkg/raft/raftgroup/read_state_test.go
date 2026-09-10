package raftgroup_test

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestReadLeaderState(t *testing.T) {
	rg := raftgroup.New(raftgroup.NewOptions(raftgroup.WithTickInterval(time.Hour)))
	require.NoError(t, rg.Start())
	defer rg.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state, err := rg.ReadLeaderState(ctx, "idle")
	require.NoError(t, err)
	require.False(t, state.Exists)
	n := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey("channel"), raft.WithNodeId(1)))
	rg.AddRaft(n)
	cfg := types.Config{Leader: 1, Term: 2, Version: 10, Role: types.RoleLeader, Replicas: []uint64{1, 2}}
	require.NoError(t, rg.AddEventWait("channel", types.Event{Type: types.ConfChange, Config: cfg}))
	state, err = rg.ReadLeaderState(ctx, "channel")
	require.NoError(t, err)
	require.True(t, state.Exists)
	require.True(t, state.Ready)
	require.Equal(t, uint64(1), state.LeaderID)
	require.Equal(t, uint32(2), state.Term)
	require.Equal(t, uint64(10), state.ConfigVersion)
	cfg.Leader, cfg.Term, cfg.Version, cfg.Role = 2, 3, 11, types.RoleFollower
	require.NoError(t, rg.AddEventWait("channel", types.Event{Type: types.ConfChange, Config: cfg}))
	state, err = rg.ReadLeaderState(ctx, "channel")
	require.NoError(t, err)
	require.False(t, state.Ready)
	require.Equal(t, uint32(3), state.Term)
	cancel()
	_, err = rg.ReadLeaderState(ctx, "channel")
	require.ErrorIs(t, err, context.Canceled)
}

func TestReadLeaderStateUnstartedAndStopped(t *testing.T) {
	rg := raftgroup.New(raftgroup.NewOptions(raftgroup.WithTickInterval(time.Hour)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := rg.ReadLeaderState(ctx, "channel")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	rg.Stop()
	_, err = rg.ReadLeaderState(context.Background(), "channel")
	require.ErrorContains(t, err, "stopped")
}
