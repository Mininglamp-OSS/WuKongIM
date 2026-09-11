package clusterconfig_test

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

// Exercise the real apply/save/promotion path with two independent Pebble stores.
// The joining node starts as a learner and must become a voter without using a
// VoteReq as a substitute for applying the membership change.
func TestSingleVoterPromotesLearnerAndKeepsServing(t *testing.T) {
	transport := newTestTransport()
	s1 := clusterconfig.New(newTestOptions(t, 1, map[uint64]string{1: ""}, clusterconfig.WithTransport(transport)))
	s2 := clusterconfig.New(newTestOptions(t, 2, nil, clusterconfig.WithSeed("1@127.0.0.1:11110"), clusterconfig.WithTransport(transport)))
	transport.serverMap[1] = s1
	transport.serverMap[2] = s2
	require.NoError(t, s1.Start())
	defer s1.Stop()
	require.NoError(t, s2.Start())
	defer s2.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.True(t, waitHasLeader(ctx, s1))
	require.NoError(t, s1.ProposeConfig(&pb.Config{Nodes: []*pb.Node{{Id: 1, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: pb.NodeStatus_NodeStatusJoined}}}))
	require.NoError(t, s1.ProposeJoin(&pb.Node{Id: 2, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: pb.NodeStatus_NodeStatusWillJoin}))
	require.Eventually(t, func() bool {
		node := s2.Node(2)
		return node != nil && node.Status == pb.NodeStatus_NodeStatusJoining
	}, 10*time.Second, 20*time.Millisecond)
	require.True(t, s1.IsLeader(), "promotion must retain the existing eligible leader")
	require.Equal(t, uint64(1), s2.LeaderId())
	// With two voters this write requires both nodes, proving promotion did not
	// merely update an in-memory status while leaving replication stranded.
	require.NoError(t, s1.ProposeApiServerAddr(1, "http://127.0.0.1:5001"))
	require.Eventually(t, func() bool {
		node := s2.Node(1)
		return node != nil && node.ApiServerAddr == "http://127.0.0.1:5001"
	}, 5*time.Second, 20*time.Millisecond)
}
