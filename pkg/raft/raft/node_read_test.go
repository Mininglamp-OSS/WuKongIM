package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestLeaderReadReady(t *testing.T) {
	n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1})))
	require.True(t, n.LeaderReadReady())
	n.stopPropose = true
	require.False(t, n.LeaderReadReady())
	n.stopPropose = false
	n.truncating = true
	require.False(t, n.LeaderReadReady())
	n.truncating = false
	n.BecomeFollower(2, 2)
	require.False(t, n.LeaderReadReady())
	n.BecomeLearner(2, 2)
	require.False(t, n.LeaderReadReady())
}
