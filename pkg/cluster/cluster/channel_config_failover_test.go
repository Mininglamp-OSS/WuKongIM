package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// Real TCP and Pebble: after losing one of three replicas, the surviving
// follower must keep pulling logs when it receives the new leader's config.
func TestChannelReplicationAfterLeaderConfigChange(t *testing.T) {
	previousTrace := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previousTrace) })
	addresses := make(map[uint64]string)
	for id := uint64(1); id <= 3; id++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addresses[id] = listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	servers := make(map[uint64]*Server)
	active := make(map[uint64]bool)
	t.Cleanup(func() {
		for id := uint64(1); id <= 3; id++ {
			if active[id] {
				servers[id].Stop()
			}
		}
	})
	for id := uint64(1); id <= 3; id++ {
		cfg := newTestOptions(t, id, addresses, clusterconfig.WithSlotCount(8), clusterconfig.WithSlotMaxReplicaCount(3))
		s := New(NewOptions(WithAddr("tcp://"+addresses[id]), WithConfigOptions(cfg), WithDataDir(t.TempDir()),
			WithDBWKDbShardNum(1), WithDBSlotShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
		require.NoError(t, s.Start())
		servers[id], active[id] = s, true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for id := uint64(1); id <= 3; id++ {
		require.NoError(t, servers[id].WaitAllSlotReady(ctx, 8))
	}
	var channelID string
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("config-failover-%d", i)
		if servers[2].cfgServer.SlotLeaderId(servers[2].getSlotId(candidate)) == 2 {
			channelID = candidate
			break
		}
	}
	require.NotEmpty(t, channelID)
	cfg := wkdb.ChannelClusterConfig{ChannelId: channelID, ChannelType: 2, LeaderId: 1, Term: 1,
		ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}, Status: wkdb.ChannelClusterStatusNormal}
	version, err := servers[2].store.SaveChannelClusterConfig(cfg)
	require.NoError(t, err)
	cfg.ConfVersion = version
	require.NoError(t, servers[1].channelServer.WakeLeaderIfNeed(cfg))
	appendMessage := func(id int64) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := servers[2].store.AppendMessages(ctx, channelID, 2, []wkdb.Message{{RecvPacket: wkproto.RecvPacket{
			ChannelID: channelID, ChannelType: 2, MessageID: id, FromUID: "config-failover", Payload: []byte(fmt.Sprintf("message-%d", id)),
		}}})
		require.NoError(t, err, "message %d must reach quorum while the old leader remains stopped", id)
	}
	for id := int64(1); id <= 3; id++ {
		appendMessage(id)
	}
	require.Eventually(t, func() bool {
		for _, s := range servers {
			seq, _, err := s.db.GetChannelLastMessageSeq(channelID, 2)
			if err != nil || seq != 3 {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond, "all initial replicas must have the same message tail")
	servers[1].Stop()
	active[1] = false
	require.Eventually(t, func() bool {
		for _, id := range []uint64{2, 3} {
			s := servers[id]
			if s.cfgServer.NodeIsOnline(1) {
				return false
			}
			for _, slot := range s.cfgServer.Slots() {
				if slot.Leader == 1 {
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond)
	for id := int64(4); id <= 7; id++ {
		appendMessage(id)
	}
	for _, id := range []uint64{2, 3} {
		messages, err := servers[id].db.LoadLastMsgs(channelID, 2, 10)
		require.NoError(t, err)
		require.Len(t, messages, 7, "both surviving replicas must persist the complete history")
		for i, message := range messages {
			require.Equal(t, uint32(i+1), message.MessageSeq)
			require.Equal(t, int64(i+1), message.MessageID)
			require.Equal(t, []byte(fmt.Sprintf("message-%d", i+1)), message.Payload)
		}
	}
	// Restart the old leader from its original database. It must rejoin as a
	// replica of the current leader, catch up, and retain subsequent writes.
	restartOpts := *servers[1].opts
	restartOpts.ChannelTransport = nil
	restartOpts.SlotTransport = nil
	restarted := New(&restartOpts)
	require.NoError(t, restarted.Start())
	servers[1], active[1] = restarted, true
	require.Eventually(t, func() bool {
		return servers[2].cfgServer.NodeIsOnline(1) && servers[3].cfgServer.NodeIsOnline(1)
	}, 20*time.Second, 100*time.Millisecond)
	appendMessage(8)
	require.Eventually(t, func() bool {
		for _, s := range servers {
			messages, err := s.db.LoadLastMsgs(channelID, 2, 10)
			if err != nil || len(messages) != 8 {
				return false
			}
			for i, message := range messages {
				if message.MessageSeq != uint32(i+1) || message.MessageID != int64(i+1) ||
					string(message.Payload) != fmt.Sprintf("message-%d", i+1) {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond, "all three replicas must retain the complete history after rejoin")
}
