//go:build integration

package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/usecase/user"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	raftstorage "github.com/WuKongIM/WuKongIM/pkg/raftlog"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/slot/proxy"
	"github.com/stretchr/testify/require"
)

// TestTokenOnNonReplicaNodes exercises the real HTTP -> user -> proxy -> Raft
// path with fresh databases, including nodes with no local runtime for the UID.
func TestTokenOnNonReplicaNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	configs := make([]raftcluster.NodeConfig, 5)
	listeners := make([]net.Listener, len(configs))
	for i := range configs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = ln.Close() })
		listeners[i] = ln
		configs[i] = raftcluster.NodeConfig{NodeID: multiraft.NodeID(i + 1), Addr: ln.Addr().String()}
	}
	for _, ln := range listeners {
		require.NoError(t, ln.Close())
	}
	clusters := make([]*raftcluster.Cluster, len(configs))
	stores := make([]*proxy.Store, len(configs))
	servers := make([]*Server, len(configs))
	for i := range configs {
		dir := t.TempDir()
		db, err := metadb.Open(filepath.Join(dir, "meta"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		raftDB, err := raftstorage.Open(filepath.Join(dir, "raft"), raftstorage.Options{})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, raftDB.Close()) })
		c, err := raftcluster.NewCluster(raftcluster.Config{
			NodeID: configs[i].NodeID, ListenAddr: configs[i].Addr, Nodes: configs,
			SlotCount: 10, InitialSlotCount: 10, HashSlotCount: 64,
			ControllerReplicaN: 3, SlotReplicaN: 3, RaftWorkers: 1,
			ControllerMetaPath:           filepath.Join(dir, "controller-meta"),
			ControllerRaftPath:           filepath.Join(dir, "controller-raft"),
			NewStorage:                   func(id multiraft.SlotID) (multiraft.Storage, error) { return raftDB.ForSlot(uint64(id)), nil },
			NewStateMachine:              metafsm.NewStateMachineFactory(db),
			NewStateMachineWithHashSlots: metafsm.NewHashSlotStateMachineFactory(db),
		})
		require.NoError(t, err)
		clusters[i] = c
		stores[i] = proxy.New(c, db)
		servers[i] = New(Options{Users: user.New(user.Options{Users: stores[i], Devices: stores[i]})})
		require.NoError(t, c.Start())
		t.Cleanup(c.Stop)
	}
	uid := "nonreplica-token-existing-user"
	slotID := clusters[0].SlotForKey(uid)
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for _, c := range clusters {
			t.Logf("node=%d listen=%s controller=%d assignments=%v", c.NodeID(), c.Server().Listener().Addr(), c.ControllerLeaderID(), c.ListCachedAssignments())
			for _, id := range c.SlotIDs() {
				leader, err := c.LeaderOf(id)
				t.Logf("node=%d slot=%d peers=%v leader=%d error=%v", c.NodeID(), id, c.PeersForSlot(id), leader, err)
			}
		}
	})
	var peers []multiraft.NodeID
	require.Eventually(t, func() bool {
		peers = clusters[0].PeersForSlot(slotID)
		if len(peers) != 3 {
			return false
		}
		for _, c := range clusters {
			if !slices.Equal(c.PeersForSlot(slotID), peers) {
				return false
			}
			for _, id := range c.SlotIDs() {
				if slices.Contains(c.PeersForSlot(id), c.NodeID()) {
					if _, err := c.LeaderOf(id); err != nil {
						return false
					}
				}
			}
		}
		return true
	}, 120*time.Second, 100*time.Millisecond)
	require.Len(t, peers, 3)
	postToken := func(node int, uid, token string) {
		t.Helper()
		body := fmt.Sprintf(`{"uid":%q,"token":%q,"device_flag":1,"device_level":1}`, uid, token)
		req := httptest.NewRequest(http.MethodPost, "/user/token", strings.NewReader(body)).WithContext(ctx)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		servers[node].Engine().ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Code, "node=%d body=%s", node+1, resp.Body.String())
		require.JSONEq(t, `{"status":200}`, resp.Body.String())
	}
	// Register on a replica, then update the same account on every ingress node.
	postToken(int(peers[0])-1, uid, "registered")
	nonreplicas := 0
	for i, c := range clusters {
		require.Equal(t, peers, c.PeersForSlot(slotID))
		token := fmt.Sprintf("token-from-node-%d", c.NodeID())
		postToken(i, uid, token)
		if !slices.Contains(peers, c.NodeID()) {
			nonreplicas++
			_, err := c.LeaderOf(slotID)
			require.ErrorIs(t, err, raftcluster.ErrSlotNotFound)
			require.ErrorIs(t, c.ProposeLocalWithHashSlot(ctx, slotID, c.HashSlotForKey(uid), nil), raftcluster.ErrSlotNotFound)
		}
		for _, store := range stores {
			device, err := store.GetDevice(ctx, uid, 1)
			require.NoError(t, err)
			require.Equal(t, token, device.Token)
		}
		// Fresh registration also needs CreateUser to route off a non-replica.
		freshUID := ""
		for suffix := 0; suffix < 10000; suffix++ {
			candidate := fmt.Sprintf("fresh-node-%d-%d", i+1, suffix)
			if c.SlotForKey(candidate) == slotID {
				freshUID = candidate
				break
			}
		}
		require.NotEmpty(t, freshUID, "find a fresh UID in the same slot")
		postToken(i, freshUID, "fresh-token")
		// Local leader scans must skip absent slots, not fail or scan remotely.
		_, err := stores[i].ListRunnableChannelMigrationTasksForLocalLeaderSlots(ctx, time.Now().UnixMilli(), 100)
		require.NoError(t, err)
	}
	require.Equal(t, 2, nonreplicas)

	// Move the actual Raft leader, then write through both non-replicas again.
	// The caller does not refresh or inject a leader into the routing layer.
	oldLeader, err := clusters[int(peers[0])-1].LeaderOf(slotID)
	require.NoError(t, err)
	newLeader := peers[0]
	if newLeader == oldLeader {
		newLeader = peers[1]
	}
	transferCtx, transferCancel := context.WithTimeout(ctx, 10*time.Second)
	defer transferCancel()
	require.NoError(t, clusters[int(oldLeader)-1].TransferSlotLeader(transferCtx, uint32(slotID), newLeader))
	require.Eventually(t, func() bool {
		leader, err := clusters[int(newLeader)-1].LeaderOf(slotID)
		return err == nil && leader == newLeader
	}, 10*time.Second, 20*time.Millisecond)
	for i, c := range clusters {
		if slices.Contains(peers, c.NodeID()) {
			continue
		}
		token := fmt.Sprintf("after-transfer-node-%d", c.NodeID())
		postToken(i, uid, token)
		for _, store := range stores {
			device, err := store.GetDevice(ctx, uid, 1)
			require.NoError(t, err)
			require.Equal(t, token, device.Token)
		}
	}
	t.Logf("slot=%d peers=%v leader=%d->%d: 7 token updates / 35 authoritative reads / 5 fresh registrations / 5 local scans", slotID, peers, oldLeader, newLeader)
}
