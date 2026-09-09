package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
	"github.com/stretchr/testify/require"
)

type proposalRouteTestHandler func(context.Context, uint8, []byte) ([]byte, error)

func newProposalRouteTestCluster(t *testing.T, handlers map[multiraft.NodeID]proposalRouteTestHandler) *Cluster {
	t.Helper()
	rt, err := multiraft.New(multiraft.Options{
		NodeID: 1, TickInterval: 10 * time.Millisecond, Workers: 1, Transport: observerTestTransport{},
		Raft: multiraft.RaftOptions{ElectionTick: 3, HeartbeatTick: 1},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Close()) })
	var nodes []NodeConfig
	for id, handler := range handlers {
		srv := transport.NewServer()
		mux := transport.NewRPCMux()
		for _, service := range []uint8{rpcServiceForward, rpcServiceManagedSlot} {
			mux.Handle(service, func(ctx context.Context, body []byte) ([]byte, error) { return handler(ctx, service, body) })
		}
		srv.HandleRPCMux(mux)
		require.NoError(t, srv.Start("127.0.0.1:0"))
		t.Cleanup(srv.Stop)
		nodes = append(nodes, NodeConfig{NodeID: id, Addr: srv.Listener().Addr().String()})
	}
	pool := transport.NewPool(NewStaticDiscovery(nodes), 2, time.Second)
	t.Cleanup(pool.Close)
	client := transport.NewClient(pool)
	t.Cleanup(client.Stop)
	c := &Cluster{
		cfg: Config{NodeID: 1}, runtime: rt, router: NewRouter(NewHashSlotTable(4, 1), 1, rt),
		transportResources: transportResources{fwdClient: client},
		agentResources:     agentResources{assignments: newAssignmentCache()},
	}
	c.slotMgr = newSlotManager(c)
	c.assignments.SetAssignments([]controllermeta.SlotAssignment{{SlotID: 1, DesiredPeers: []uint64{2, 3}, ConfigEpoch: 1}})
	return c
}

func proposalRouteTestStatus(leader uint64) ([]byte, error) {
	return encodeManagedSlotResponse(managedSlotRPCResponse{LeaderID: leader, CurrentVoters: []uint64{2, 3}})
}

func setProposalRouteTestObservation(c *Cluster, view controllermeta.SlotRuntimeView) {
	c.agent = &slotAgent{observationState: observationAppliedState{
		LeaderID: 2, LeaderGeneration: 1,
		RuntimeViews: map[uint32]controllermeta.SlotRuntimeView{1: view},
	}}
}

func TestProposalRoutesWithoutLocalReplica(t *testing.T) {
	var writes atomic.Int32
	c := newProposalRouteTestCluster(t, map[multiraft.NodeID]proposalRouteTestHandler{
		2: func(_ context.Context, service uint8, _ []byte) ([]byte, error) {
			if service != rpcServiceManagedSlot {
				return nil, errors.New("must not propose to follower")
			}
			return proposalRouteTestStatus(3)
		},
		3: func(_ context.Context, service uint8, body []byte) ([]byte, error) {
			if service == rpcServiceManagedSlot {
				return proposalRouteTestStatus(3)
			}
			writes.Add(1)
			slot, payload, err := decodeForwardPayload(body)
			if err != nil || slot != 1 || string(payload) != string(encodeProposalPayload(2, []byte("command"))) {
				return nil, errors.New("lost slot/hash-slot proposal envelope")
			}
			return encodeForwardResp(errCodeOK, []byte("applied-result")), nil
		},
	})
	result, err := c.ProposeWithHashSlotResult(context.Background(), 1, 2, []byte("command"))
	require.NoError(t, err)
	require.Equal(t, "applied-result", string(result))
	require.EqualValues(t, 1, writes.Load())
	_, err = c.LeaderOf(1)
	require.ErrorIs(t, err, ErrSlotNotFound)
	require.ErrorIs(t, c.ProposeLocalWithHashSlot(context.Background(), 1, 2, nil), ErrSlotNotFound)
	require.EqualValues(t, 1, writes.Load(), "local-only proposal must not forward")
	require.ErrorIs(t, c.ProposeWithHashSlot(context.Background(), 99, 2, nil), ErrSlotNotFound)
}

func TestProposalRefreshesRejectedObservation(t *testing.T) {
	for _, test := range []struct {
		name string
		code byte
	}{
		{"not_leader", errCodeNotLeader}, {"no_slot", errCodeNoSlot},
	} {
		t.Run(test.name, func(t *testing.T) {
			var oldWrites, newWrites atomic.Int32
			c := newProposalRouteTestCluster(t, map[multiraft.NodeID]proposalRouteTestHandler{
				2: func(_ context.Context, service uint8, _ []byte) ([]byte, error) {
					if service == rpcServiceManagedSlot {
						return proposalRouteTestStatus(3)
					}
					oldWrites.Add(1)
					return encodeForwardResp(test.code, nil), nil
				},
				3: func(_ context.Context, service uint8, _ []byte) ([]byte, error) {
					if service == rpcServiceManagedSlot {
						return proposalRouteTestStatus(3)
					}
					newWrites.Add(1)
					return encodeForwardResp(errCodeOK, []byte("new-leader")), nil
				},
			})
			setProposalRouteTestObservation(c, controllermeta.SlotRuntimeView{
				SlotID: 1, LeaderID: 2, CurrentVoters: []uint64{2, 3}, ObservedConfigEpoch: 1, LastReportAt: time.Now(),
			})
			result, err := c.ProposeWithHashSlotResult(context.Background(), 1, 2, nil)
			require.NoError(t, err)
			require.Equal(t, "new-leader", string(result))
			require.EqualValues(t, 1, oldWrites.Load())
			require.EqualValues(t, 1, newWrites.Load())
		})
	}
}

func TestProposalObservationConcurrentDelta(t *testing.T) {
	c := newProposalRouteTestCluster(t, nil)
	view := controllermeta.SlotRuntimeView{SlotID: 1, LeaderID: 2, CurrentVoters: []uint64{2, 3}, ObservedConfigEpoch: 1, LastReportAt: time.Now()}
	setProposalRouteTestObservation(c, view)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			c.agent.observationMu.Lock()
			applyObservationDelta(&c.agent.observationState, observationDeltaResponse{
				FullSync: true, LeaderID: 2, LeaderGeneration: 1,
				RuntimeViews: []controllermeta.SlotRuntimeView{view},
			})
			c.agent.observationMu.Unlock()
		}
	}()
	defer wg.Wait()
	for i := 0; i < 1000; i++ {
		require.EqualValues(t, 2, c.observedProposalLeader(1, []multiraft.NodeID{2, 3}))
	}
}

func TestProposalObservationValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*controllermeta.SlotRuntimeView)
		want   multiraft.NodeID
	}{
		{"fresh", func(*controllermeta.SlotRuntimeView) {}, 2},
		{"expired", func(v *controllermeta.SlotRuntimeView) { v.LastReportAt = time.Now().Add(-time.Hour) }, 3},
		{"future", func(v *controllermeta.SlotRuntimeView) { v.LastReportAt = time.Now().Add(time.Hour) }, 3},
		{"no_timestamp", func(v *controllermeta.SlotRuntimeView) { v.LastReportAt = time.Time{} }, 3},
		{"stale_epoch", func(v *controllermeta.SlotRuntimeView) { v.ObservedConfigEpoch = 0 }, 3},
		{"removed_leader", func(v *controllermeta.SlotRuntimeView) { v.LeaderID = 4; v.CurrentVoters = []uint64{4} }, 3},
		{"not_voter", func(v *controllermeta.SlotRuntimeView) { v.CurrentVoters = []uint64{3} }, 3},
		{"unknown_leader", func(v *controllermeta.SlotRuntimeView) { v.LeaderID = 0 }, 3},
		{"wrong_slot", func(v *controllermeta.SlotRuntimeView) { v.SlotID = 2 }, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newProposalRouteTestCluster(t, nil)
			view := controllermeta.SlotRuntimeView{SlotID: 1, LeaderID: 2, CurrentVoters: []uint64{2, 3}, ObservedConfigEpoch: 1, LastReportAt: time.Now()}
			test.change(&view)
			setProposalRouteTestObservation(c, view)
			c.setManagedSlotStatusTestHook(func(_ *Cluster, _ multiraft.NodeID, _ multiraft.SlotID) (managedSlotStatus, error, bool) {
				return managedSlotStatus{LeaderID: 3, CurrentVoters: []multiraft.NodeID{2, 3}}, nil, true
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			leader, err := c.resolveProposalLeader(ctx, 1, false)
			require.NoError(t, err)
			require.Equal(t, test.want, leader)
		})
	}
}

func TestProposalDiscoveryDoesNotInventLeader(t *testing.T) {
	for _, status := range []managedSlotStatus{
		{},
		{LeaderID: 4, CurrentVoters: []multiraft.NodeID{2, 3, 4}},
		{LeaderID: 2, CurrentVoters: []multiraft.NodeID{3}},
	} {
		c := newProposalRouteTestCluster(t, nil)
		c.setManagedSlotStatusTestHook(func(_ *Cluster, _ multiraft.NodeID, _ multiraft.SlotID) (managedSlotStatus, error, bool) {
			return status, nil, true
		})
		_, err := c.resolveProposalLeader(context.Background(), 1, false)
		require.ErrorIs(t, err, ErrNoLeader)
	}
}

func TestProposalDiscoveryCancellationAndBudget(t *testing.T) {
	c := newProposalRouteTestCluster(t, nil)
	c.cfg.Timeouts.ForwardRetryBudget = 60 * time.Millisecond
	var calls atomic.Int32
	c.setManagedSlotStatusTestHook(func(_ *Cluster, _ multiraft.NodeID, _ multiraft.SlotID) (managedSlotStatus, error, bool) {
		calls.Add(1)
		return managedSlotStatus{}, ErrSlotNotFound, true
	})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, c.ProposeWithHashSlot(canceled, 1, 2, nil), context.Canceled)
	require.Zero(t, calls.Load())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err := c.ProposeWithHashSlot(ctx, 1, 2, nil)
	require.Error(t, err)
	require.Less(t, time.Since(start), 500*time.Millisecond, "must not use the caller's 5s as the routing budget")
	require.NoError(t, ctx.Err())
	require.Positive(t, calls.Load())
}

func TestProposalDiscoverySkipsSlowPeer(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	c := newProposalRouteTestCluster(t, map[multiraft.NodeID]proposalRouteTestHandler{
		2: func(context.Context, uint8, []byte) ([]byte, error) {
			<-release
			return proposalRouteTestStatus(2)
		},
		3: func(_ context.Context, service uint8, _ []byte) ([]byte, error) {
			if service == rpcServiceManagedSlot {
				return proposalRouteTestStatus(3)
			}
			return encodeForwardResp(errCodeOK, nil), nil
		},
	})
	require.NoError(t, c.ProposeWithHashSlot(context.Background(), 1, 2, nil))
}

func TestProposalDoesNotRetryAmbiguousForwardTimeout(t *testing.T) {
	var writes atomic.Int32
	c := newProposalRouteTestCluster(t, map[multiraft.NodeID]proposalRouteTestHandler{
		2: func(_ context.Context, service uint8, _ []byte) ([]byte, error) {
			if service == rpcServiceManagedSlot {
				return proposalRouteTestStatus(2)
			}
			writes.Add(1)
			return encodeForwardResp(errCodeTimeout, nil), nil
		},
	})
	require.ErrorIs(t, c.ProposeWithHashSlot(context.Background(), 1, 2, nil), transport.ErrTimeout)
	require.EqualValues(t, 1, writes.Load())
}
