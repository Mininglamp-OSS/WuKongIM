package cluster

import (
	"context"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
	"net"
	"sync"
	"testing"
	"time"
)

func newConversationConsistencyServer(t *testing.T, decorate func(*Server)) (*Server, context.Context) {
	t.Helper()
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	config := newTestOptions(t, 1, nil, clusterconfig.WithSlotCount(1), clusterconfig.WithSlotMaxReplicaCount(1))
	s := New(NewOptions(WithAddr("tcp://"+address), WithConfigOptions(config), WithDataDir(t.TempDir()),
		WithDBWKDbShardNum(1), WithDBSlotShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
	if decorate != nil {
		decorate(s)
	}
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, s.WaitAllSlotReady(ctx, 1))
	return s, ctx
}
func consistencyConfig() wkdb.ChannelClusterConfig {
	return wkdb.ChannelClusterConfig{ChannelId: "analysis-channel", ChannelType: 2, LeaderId: 1, Term: 1,
		ConfVersion: 1, ReplicaMaxCount: 1, Replicas: []uint64{1}, Status: wkdb.ChannelClusterStatusNormal}
}
func consistencyMessage(seq uint32) wkdb.Message {
	return wkdb.Message{Term: 1, RecvPacket: wkproto.RecvPacket{ChannelID: "analysis-channel", ChannelType: 2,
		MessageID: int64(seq), MessageSeq: seq, Payload: []byte("diagnostic")}}
}

type delayedConversationDB struct {
	wkdb.DB
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *delayedConversationDB) AddOrUpdateConversationsWithUser(uid string, c []wkdb.Conversation) error {
	if uid == "analysis-blocked" {
		d.once.Do(func() { close(d.entered) })
		<-d.release
	}
	return d.DB.AddOrUpdateConversationsWithUser(uid, c)
}
func TestConversationSlotApplyLag(t *testing.T) {
	var db *delayedConversationDB
	s, ctx := newConversationConsistencyServer(t, func(s *Server) {
		db = &delayedConversationDB{DB: s.db, entered: make(chan struct{}), release: make(chan struct{})}
		s.store = store.New(store.NewOptions(store.WithNodeId(1), store.WithSlot(s.slotServer), store.WithChannel(s.channelServer), store.WithDB(db)))
	})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(db.release) }) }
	t.Cleanup(release)
	cfg := consistencyConfig()
	require.NoError(t, s.db.SaveChannelClusterConfig(cfg))
	require.NoError(t, s.db.AppendMessages(cfg.ChannelId, cfg.ChannelType, []wkdb.Message{consistencyMessage(42)}))
	seq, err := s.GetChannelLastMessageSeq(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(42), seq)
	done := make(chan error, 1)
	go func() {
		done <- s.store.AddOrUpdateUserConversations("analysis-blocked", []wkdb.Conversation{{Uid: "analysis-blocked", ChannelId: "unrelated", ChannelType: 2, Type: wkdb.ConversationTypeChat, ReadToMsgSeq: 1}})
	}()
	select {
	case <-db.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	state, err := s.slotServer.ReadLeaderState(ctx, 0)
	require.NoError(t, err)
	require.Less(t, state.AppliedIndex, state.CommittedIndex)
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	type readResult struct {
		seq uint64
		err error
	}
	result := make(chan readResult, 1)
	go func() {
		seq, err := s.GetChannelLastMessageSeq(readCtx, cfg.ChannelId, cfg.ChannelType)
		result <- readResult{seq, err}
	}()
	select {
	case r := <-result:
		t.Fatalf("returned before apply despite available budget: %+v", r)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case r := <-result:
		require.NoError(t, r.err)
		require.Equal(t, uint64(42), r.seq)
	case <-readCtx.Done():
		t.Fatal(readCtx.Err())
	}
	require.Eventually(t, func() bool {
		st, err := s.slotServer.ReadLeaderState(ctx, 0)
		return err == nil && st.AppliedIndex >= state.CommittedIndex
	}, time.Second, time.Millisecond)
	seq, err = s.GetChannelLastMessageSeq(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(42), seq)
	t.Logf("read waited for unrelated apply; sequence=%d", seq)
}

type conversationConfigHookDB struct {
	wkdb.DB
	mu   sync.Mutex
	hook func()
}

func (d *conversationConfigHookDB) GetChannelClusterConfig(id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
	cfg, err := d.DB.GetChannelClusterConfig(id, typ)
	d.mu.Lock()
	hook := d.hook
	d.hook = nil
	d.mu.Unlock()
	if hook != nil {
		hook()
	}
	return cfg, err
}
func TestConversationConcurrentSlotWrite(t *testing.T) {
	var db *conversationConfigHookDB
	s, ctx := newConversationConsistencyServer(t, func(s *Server) { db = &conversationConfigHookDB{DB: s.db}; s.db = db })
	cfg := consistencyConfig()
	require.NoError(t, s.db.SaveChannelClusterConfig(cfg))
	before, err := s.slotServer.ReadLeaderState(ctx, 0)
	require.NoError(t, err)
	originalConfig, err := db.DB.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	db.mu.Lock()
	db.hook = func() {
		require.NoError(t, s.store.AddOrUpdateUserConversations("analysis-other", []wkdb.Conversation{{Uid: "analysis-other", ChannelId: "unrelated", ChannelType: 2, Type: wkdb.ConversationTypeChat, ReadToMsgSeq: 1}}))
		require.Eventually(t, func() bool {
			st, err := s.slotServer.ReadLeaderState(ctx, 0)
			return err == nil && st.AppliedIndex > before.AppliedIndex
		}, time.Second, time.Millisecond)
	}
	db.mu.Unlock()
	_, err = s.loadConversationConfigLocal(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	after, stateErr := s.slotServer.ReadLeaderState(ctx, 0)
	require.NoError(t, stateErr)
	unchanged, loadErr := db.DB.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, loadErr)
	require.True(t, originalConfig.Equal(unchanged))
	require.Equal(t, before.LeaderID, after.LeaderID)
	require.Equal(t, before.Term, after.Term)
	t.Logf("unrelated write: before committed/applied=%d/%d after=%d/%d sameLeader=%t sameTerm=%t sameChannelConfig=true readErr=%v", before.CommittedIndex, before.AppliedIndex, after.CommittedIndex, after.AppliedIndex, before.LeaderID == after.LeaderID, before.Term == after.Term, err)
}
func TestConversationUncommittedTail(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	cfg := consistencyConfig()
	cfg.ReplicaMaxCount = 3
	cfg.Replicas = []uint64{1, 2, 3}
	require.NoError(t, s.db.SaveChannelClusterConfig(cfg))
	require.NoError(t, s.db.AppendMessages(cfg.ChannelId, cfg.ChannelType, []wkdb.Message{consistencyMessage(41)}))
	require.NoError(t, s.channelServer.WakeLeaderIfNeed(cfg))
	before, err := s.channelServer.ReadLeaderState(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(41), before.CommittedIndex)
	msg := consistencyMessage(42)
	data, err := msg.Marshal()
	require.NoError(t, err)
	// Run the actual Raft append/store pipeline, with no follower ACKs for index 42.
	s.channelServer.AddEvent(wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType), rafttypes.Event{Type: rafttypes.Propose, Logs: []rafttypes.Log{{Id: 42, Index: 42, Term: 1, Data: data}}})
	require.Eventually(t, func() bool {
		tail, _, err := s.db.GetChannelLastMessageSeq(cfg.ChannelId, cfg.ChannelType)
		return err == nil && tail == 42
	}, time.Second, time.Millisecond)
	state, err := s.channelServer.ReadLeaderState(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(41), state.CommittedIndex)
	seq, err := s.GetChannelLastMessageSeq(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(41), seq)
	t.Logf("committed boundary: exists=%t ready=%t committed=%d applied=%d dbTail=42 returnedSeq=%d err=%v", state.Exists, state.Ready, state.CommittedIndex, state.AppliedIndex, seq, err)
}

func TestConversationReadDuringSlotWrites(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	cfg := consistencyConfig()
	require.NoError(t, s.db.SaveChannelClusterConfig(cfg))
	require.NoError(t, s.db.AppendMessages(cfg.ChannelId, cfg.ChannelType, []wkdb.Message{consistencyMessage(42)}))
	require.NoError(t, s.channelServer.WakeLeaderIfNeed(cfg))
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		for i := uint64(1); i <= 40; i++ {
			err := s.store.AddOrUpdateUserConversations("busy-user", []wkdb.Conversation{{Uid: "busy-user", ChannelId: "other-channel", ChannelType: 2, Type: wkdb.ConversationTypeChat, ReadToMsgSeq: i}})
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	<-started
	for i := 0; i < 20; i++ {
		seq, err := s.GetChannelLastMessageSeq(ctx, cfg.ChannelId, cfg.ChannelType)
		require.NoError(t, err)
		require.Equal(t, uint64(42), seq)
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
