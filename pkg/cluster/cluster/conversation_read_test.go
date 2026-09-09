package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func boundaryConfig() wkdb.ChannelClusterConfig {
	return wkdb.ChannelClusterConfig{ChannelId: "alice@bob", ChannelType: 1, LeaderId: 4, Term: 2,
		ConfVersion: 10, Replicas: []uint64{4, 5}, Status: wkdb.ChannelClusterStatusNormal}
}

func boundaryReader(t *testing.T, cfg *wkdb.ChannelClusterConfig) conversationReader {
	t.Helper()
	return conversationReader{
		nodeID: 1,
		load: func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
			require.Equal(t, cfg.ChannelId, id)
			require.Equal(t, cfg.ChannelType, typ)
			return *cfg, ctx.Err()
		},
		state: func(context.Context, string, uint8) (raftgroup.ReadState, error) {
			return raftgroup.ReadState{Exists: true, Ready: true, LeaderID: cfg.LeaderId, Term: cfg.Term, ConfigVersion: cfg.ConfVersion}, nil
		},
		local: func(string, uint8) (uint64, uint64, error) {
			t.Fatal("must not read the UID owner's local tail")
			return 0, 0, nil
		},
		remote: func(ctx context.Context, node uint64, path string, req conversationReadRequest) (conversationReadResponse, error) {
			require.Equal(t, cfg.LeaderId, node)
			require.Equal(t, conversationBoundaryPath, path)
			require.Equal(t, *cfg, req.Expected)
			seq, copy := uint64(42), *cfg
			return conversationReadResponse{Version: 1, Sequence: &seq, Config: &copy}, ctx.Err()
		},
	}
}

func TestConversationReaderRemote(t *testing.T) {
	cfg := boundaryConfig()
	r := boundaryReader(t, &cfg)
	seq, err := r.latest(context.Background(), cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(42), seq)
}

func TestConversationReaderRefreshesRoute(t *testing.T) {
	for _, trigger := range []string{"leader", "term", "config", "transport", "metadata", "timeout"} {
		t.Run(trigger, func(t *testing.T) {
			cfg := boundaryConfig()
			r := boundaryReader(t, &cfg)
			calls := 0
			remote, load := r.remote, r.load
			if trigger == "metadata" {
				r.load = func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
					calls++
					if calls == 1 {
						return wkdb.EmptyChannelClusterConfig, errors.New("slot leader unavailable")
					}
					return load(ctx, id, typ)
				}
			} else {
				r.remote = func(ctx context.Context, node uint64, path string, req conversationReadRequest) (conversationReadResponse, error) {
					calls++
					resp, err := remote(ctx, node, path, req)
					if calls == 1 {
						switch trigger {
						case "leader":
							cfg.LeaderId = 5
						case "term":
							cfg.Term++
						case "config":
							cfg.ConfVersion++
						case "transport":
							cfg.LeaderId = 5
							return conversationReadResponse{}, errors.New("connection reset")
						case "timeout":
							<-ctx.Done()
							cfg.LeaderId = 5
							return conversationReadResponse{}, ctx.Err()
						}
					}
					return resp, err
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			seq, err := r.latest(ctx, cfg.ChannelId, cfg.ChannelType)
			require.NoError(t, err)
			require.Equal(t, uint64(42), seq)
			if trigger != "metadata" {
				require.Equal(t, 2, calls)
			}
		})
	}
}

func TestConversationReaderFailsClosed(t *testing.T) {
	for _, failure := range []string{"old peer", "missing sequence", "missing config", "wrong epoch", "transport", "churn", "disappeared", "candidate", "no leader", "learner leader", "storage"} {
		t.Run(failure, func(t *testing.T) {
			cfg := boundaryConfig()
			r := boundaryReader(t, &cfg)
			calls := 0
			remote, load := r.remote, r.load
			r.remote = func(ctx context.Context, node uint64, path string, req conversationReadRequest) (conversationReadResponse, error) {
				calls++
				resp, err := remote(ctx, node, path, req)
				switch failure {
				case "old peer":
					resp.Version = 0
				case "missing sequence":
					resp.Sequence = nil
				case "missing config":
					resp.Config = nil
				case "wrong epoch":
					resp.Config.Term++
				case "transport":
					err = errors.New("unavailable")
				case "churn":
					cfg.Term++
				}
				return resp, err
			}
			switch failure {
			case "disappeared":
				r.load = func(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
					if calls > 0 {
						return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
					}
					return load(ctx, id, typ)
				}
			case "candidate":
				cfg.Status = wkdb.ChannelClusterStatusCandidate
			case "no leader":
				cfg.LeaderId = 0
			case "learner leader":
				cfg.Learners = []uint64{cfg.LeaderId}
			case "storage":
				r.nodeID = cfg.LeaderId
				r.local = func(string, uint8) (uint64, uint64, error) { return 0, 0, errors.New("disk error") }
			}
			seq, err := r.latest(context.Background(), cfg.ChannelId, cfg.ChannelType)
			require.ErrorIs(t, err, ErrConversationReadRetry)
			require.Zero(t, seq)
			require.LessOrEqual(t, calls, 2)
		})
	}
}

func TestConversationReaderMissingAndCancellation(t *testing.T) {
	cfg := boundaryConfig()
	r := boundaryReader(t, &cfg)
	loads := 0
	r.load = func(ctx context.Context, _ string, _ uint8) (wkdb.ChannelClusterConfig, error) {
		loads++
		return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
	}
	seq, err := r.latest(context.Background(), cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Zero(t, seq)
	require.Equal(t, 1, loads)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = r.latest(ctx, cfg.ChannelId, cfg.ChannelType)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, loads)
	r.load = func(ctx context.Context, _ string, _ uint8) (wkdb.ChannelClusterConfig, error) {
		<-ctx.Done()
		return wkdb.EmptyChannelClusterConfig, ctx.Err()
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = r.latest(ctx, cfg.ChannelId, cfg.ChannelType)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestConversationReadServingState(t *testing.T) {
	for _, state := range []string{"leader", "idle", "follower", "wrong term", "wrong version", "draining", "removed during read", "demoted during read", "config changed", "wrong node", "idle transfer"} {
		t.Run(state, func(t *testing.T) {
			cfg := boundaryConfig()
			r := boundaryReader(t, &cfg)
			r.nodeID = cfg.LeaderId
			reads, snapshots := 0, 0
			r.local = func(string, uint8) (uint64, uint64, error) { reads++; return 42, 0, nil }
			readState := r.state
			r.state = func(ctx context.Context, id string, typ uint8) (raftgroup.ReadState, error) {
				snapshots++
				s, err := readState(ctx, id, typ)
				switch state {
				case "idle", "idle transfer":
					s.Exists = false
				case "follower":
					s.Ready = false
					s.LeaderID = 5
				case "wrong term":
					s.Term++
				case "wrong version":
					s.ConfigVersion++
				case "draining":
					s.Ready = false
				case "removed during read":
					if snapshots == 2 {
						s.Exists = false
					}
				case "demoted during read":
					if snapshots == 2 {
						s.Ready = false
					}
				case "config changed":
					if snapshots == 2 {
						cfg.Term++
					}
				}
				return s, err
			}
			if state == "wrong node" {
				r.nodeID = 1
			}
			if state == "idle transfer" {
				cfg.MigrateFrom, cfg.MigrateTo = 4, 5
			}
			seq, err := r.readLocal(context.Background(), cfg)
			if state == "leader" || state == "idle" {
				require.NoError(t, err)
				require.Equal(t, uint64(42), seq)
				require.Equal(t, 1, reads)
			} else {
				require.ErrorIs(t, err, ErrConversationReadRetry)
				require.Zero(t, seq)
				if state != "removed during read" && state != "demoted during read" && state != "config changed" {
					require.Zero(t, reads)
				}
			}
		})
	}
}
