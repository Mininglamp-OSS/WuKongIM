package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// Real TCP RPC + Pebble: the conversation owner has sequence 7 while the
// channel's sole message replica has 42. No manager HTTP endpoint is available.
func TestConversationReadRPC(t *testing.T) {
	previousTrace := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previousTrace) })
	addresses := make(map[uint64]string)
	for id := uint64(1); id <= 2; id++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addresses[id] = listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	servers := make([]*Server, 0, 2)
	for id := uint64(1); id <= 2; id++ {
		config := newTestOptions(t, id, addresses, clusterconfig.WithSlotCount(2), clusterconfig.WithSlotMaxReplicaCount(2))
		s := New(NewOptions(WithAddr("tcp://"+addresses[id]), WithConfigOptions(config), WithDataDir(t.TempDir()),
			WithDBWKDbShardNum(1), WithDBSlotShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
		require.NoError(t, s.Start())
		t.Cleanup(s.Stop)
		servers = append(servers, s)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, s := range servers {
		require.NoError(t, s.WaitAllSlotReady(ctx, 2))
	}
	owner, leader := servers[0], servers[1]
	// Choose metadata owned by node 1, forcing node 2 to fetch it over RPC.
	var id string
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("boundary-%d", i)
		if owner.cfgServer.SlotLeaderId(owner.getSlotId(candidate)) == 1 {
			id = candidate
			break
		}
	}
	require.NotEmpty(t, id)
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 2, Term: 1, ConfVersion: 1,
		ReplicaMaxCount: 1, Replicas: []uint64{2}, Status: wkdb.ChannelClusterStatusNormal}
	require.NoError(t, owner.db.SaveChannelClusterConfig(cfg))
	for i, s := range servers {
		seq := uint32(7)
		if i == 1 {
			seq = 42
		}
		require.NoError(t, s.db.AppendMessages(id, 2, []wkdb.Message{{Term: 1, RecvPacket: wkproto.RecvPacket{
			ChannelID: id, ChannelType: 2, MessageID: int64(seq), MessageSeq: seq, Payload: []byte("message"),
		}}}))
	}
	t.Run("idle leader", func(t *testing.T) {
		seq, err := owner.GetChannelLastMessageSeq(ctx, id, 2)
		require.NoError(t, err)
		require.Equal(t, uint64(42), seq)
		require.False(t, leader.channelServer.ExistChannel(id, 2), "read must not wake the channel")
	})
	t.Run("active leader", func(t *testing.T) {
		require.NoError(t, leader.channelServer.WakeLeaderIfNeed(cfg))
		seq, err := owner.GetChannelLastMessageSeq(ctx, id, 2)
		require.NoError(t, err)
		require.Equal(t, uint64(42), seq)
		seq, err = leader.GetChannelLastMessageSeq(ctx, id, 2)
		require.NoError(t, err)
		require.Equal(t, uint64(42), seq)
	})
	t.Run("stale epoch", func(t *testing.T) {
		stale := cfg
		stale.Term++
		_, err := owner.requestConversationRead(ctx, 2, conversationBoundaryPath,
			conversationReadRequest{ChannelID: id, ChannelType: 2, Expected: stale})
		require.ErrorIs(t, err, ErrConversationReadRetry)
	})
	t.Run("wrong node", func(t *testing.T) {
		_, err := leader.requestConversationRead(ctx, 1, conversationBoundaryPath,
			conversationReadRequest{ChannelID: id, ChannelType: 2, Expected: cfg})
		require.ErrorIs(t, err, ErrConversationReadRetry)
	})
	t.Run("missing metadata", func(t *testing.T) {
		seq, err := leader.GetChannelLastMessageSeq(ctx, "never-created", 2)
		require.NoError(t, err)
		require.Zero(t, seq)
		_, err = owner.db.GetChannelClusterConfig("never-created", 2)
		require.ErrorIs(t, err, wkdb.ErrNotFound)
		_, err = leader.db.GetChannelClusterConfig("never-created", 2)
		require.ErrorIs(t, err, wkdb.ErrNotFound)
	})
	t.Run("old metadata peer", func(t *testing.T) {
		owner.netServer.Route(conversationConfigPath, func(c *wkserver.Context) {
			c.WriteStatus(proto.StatusNotFound)
		})
		defer owner.netServer.Route(conversationConfigPath, func(c *wkserver.Context) { owner.rpcServer.handleConversationRead(c, true) })
		seq, err := leader.GetChannelLastMessageSeq(ctx, id, 2)
		require.ErrorIs(t, err, ErrConversationReadRetry)
		require.Zero(t, seq, "an unsupported metadata RPC is not an empty channel")
	})
	for _, body := range []string{"not supported", `{}`, `{"version":1}`, `{"version":1,"sequence":-1}`} {
		t.Run(body, func(t *testing.T) {
			leader.netServer.Route(conversationBoundaryPath, func(c *wkserver.Context) {
				if body == "not supported" {
					c.WriteStatus(proto.StatusNotFound)
				} else {
					c.Write([]byte(body))
				}
			})
			defer leader.netServer.Route(conversationBoundaryPath, func(c *wkserver.Context) { leader.rpcServer.handleConversationRead(c, false) })
			seq, err := owner.GetChannelLastMessageSeq(ctx, id, 2)
			require.ErrorIs(t, err, ErrConversationReadRetry)
			require.Zero(t, seq)
		})
	}
	t.Run("runtime follower", func(t *testing.T) {
		// Metadata still designates node 2, but its runtime has stepped down.
		leader.channelServer.AddEvent(wkutil.ChannelToKey(id, 2), rafttypes.Event{Type: rafttypes.ConfChange,
			Config: rafttypes.Config{Leader: 1, Term: 2, Version: 2, Role: rafttypes.RoleFollower, Replicas: []uint64{1, 2}}})
		require.Eventually(t, func() bool {
			state, err := leader.channelServer.ReadLeaderState(ctx, id, 2)
			return err == nil && !state.Ready && state.Term == 2
		}, time.Second, time.Millisecond)
		seq, err := owner.GetChannelLastMessageSeq(ctx, id, 2)
		require.ErrorIs(t, err, ErrConversationReadRetry)
		require.Zero(t, seq)
	})
}

func TestConversationReadSingleNode(t *testing.T) {
	previousTrace := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previousTrace) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	// Standalone deployments leave InitNodes empty so the initial leader is self.
	config := newTestOptions(t, 1, nil, clusterconfig.WithSlotCount(1), clusterconfig.WithSlotMaxReplicaCount(1))
	s := New(NewOptions(WithAddr("tcp://"+address), WithConfigOptions(config), WithDataDir(t.TempDir()),
		WithDBWKDbShardNum(1), WithDBSlotShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, s.WaitAllSlotReady(ctx, 1))
	cfg := wkdb.ChannelClusterConfig{ChannelId: "single-node", ChannelType: 2, LeaderId: 1, Term: 1, ConfVersion: 1,
		ReplicaMaxCount: 1, Replicas: []uint64{1}, Status: wkdb.ChannelClusterStatusNormal}
	require.NoError(t, s.db.SaveChannelClusterConfig(cfg))
	require.NoError(t, s.db.AppendMessages(cfg.ChannelId, cfg.ChannelType, []wkdb.Message{{Term: 1, RecvPacket: wkproto.RecvPacket{
		ChannelID: cfg.ChannelId, ChannelType: cfg.ChannelType, MessageID: 42, MessageSeq: 42, Payload: []byte("message"),
	}}}))
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprintf("active=%t", active), func(t *testing.T) {
			if active {
				require.NoError(t, s.channelServer.WakeLeaderIfNeed(cfg))
			}
			seq, err := s.GetChannelLastMessageSeq(ctx, cfg.ChannelId, cfg.ChannelType)
			require.NoError(t, err)
			require.Equal(t, uint64(42), seq)
			require.Equal(t, active, s.channelServer.ExistChannel(cfg.ChannelId, cfg.ChannelType))
		})
	}
}
