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

func TestConversationConfigFixedApplyBarrier(t *testing.T) {
	states := []raftgroup.ReadState{
		{Exists: true, Ready: true, LeaderID: 1, Term: 3, CommittedIndex: 10, AppliedIndex: 7},
		{Exists: true, Ready: true, LeaderID: 1, Term: 3, CommittedIndex: 11, AppliedIndex: 10},
		{Exists: true, Ready: true, LeaderID: 1, Term: 3, CommittedIndex: 12, AppliedIndex: 11},
	}
	snapshots, loads := 0, 0
	r := conversationConfigReader{
		nodeID: 1, leader: func() uint64 { return 1 },
		state: func(context.Context) (raftgroup.ReadState, error) {
			require.Less(t, snapshots, len(states), "must not chase later commits")
			s := states[snapshots]
			snapshots++
			return s, nil
		},
		load: func() (wkdb.ChannelClusterConfig, error) {
			require.Equal(t, 2, snapshots, "DB read must wait for the captured commit")
			loads++
			return boundaryConfig(), nil
		},
	}
	cfg, err := r.read(context.Background())
	require.NoError(t, err)
	require.Equal(t, boundaryConfig(), cfg)
	require.Equal(t, 1, loads)
	require.Equal(t, 3, snapshots)
}

func TestConversationConfigLeadershipFence(t *testing.T) {
	for _, phase := range []string{"waiting", "after DB"} {
		for _, change := range []string{"leader", "term", "version", "removed", "draining", "route"} {
			t.Run(phase+"/"+change, func(t *testing.T) {
				base := raftgroup.ReadState{Exists: true, Ready: true, LeaderID: 1, Term: 3, ConfigVersion: 4, CommittedIndex: 10, AppliedIndex: 10}
				calls, loads, route := 0, 0, uint64(1)
				r := conversationConfigReader{nodeID: 1, leader: func() uint64 { return route }}
				r.state = func(context.Context) (raftgroup.ReadState, error) {
					calls++
					s := base
					if calls == 1 && phase == "waiting" {
						s.AppliedIndex = 9
					}
					if calls > 1 {
						switch change {
						case "leader":
							s.LeaderID = 2
						case "term":
							s.Term++
						case "version":
							s.ConfigVersion++
						case "removed":
							s.Exists = false
						case "draining":
							s.Ready = false
						case "route":
							route = 2
						}
					}
					return s, nil
				}
				r.load = func() (wkdb.ChannelClusterConfig, error) { loads++; return boundaryConfig(), nil }
				_, err := r.read(context.Background())
				require.ErrorIs(t, err, ErrConversationReadRetry)
				if phase == "waiting" {
					require.Zero(t, loads)
				} else {
					require.Equal(t, 1, loads)
				}
			})
		}
	}
}

func TestConversationConfigApplyCancellation(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[timeout], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
			defer cancel()
			r := conversationConfigReader{nodeID: 1, leader: func() uint64 { return 1 }}
			r.state = func(context.Context) (raftgroup.ReadState, error) {
				if !timeout {
					cancel()
				}
				return raftgroup.ReadState{Exists: true, Ready: true, LeaderID: 1, Term: 1, CommittedIndex: 10}, nil
			}
			r.load = func() (wkdb.ChannelClusterConfig, error) {
				t.Fatal("must not read before apply")
				return wkdb.EmptyChannelClusterConfig, nil
			}
			_, err := r.read(ctx)
			if timeout {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func TestConversationConfigStorageErrors(t *testing.T) {
	for _, readErr := range []error{wkdb.ErrNotFound, errors.New("disk unavailable")} {
		r := conversationConfigReader{
			nodeID: 1, leader: func() uint64 { return 1 },
			state: func(context.Context) (raftgroup.ReadState, error) {
				return raftgroup.ReadState{Exists: true, Ready: true, LeaderID: 1, Term: 1}, nil
			},
			load: func() (wkdb.ChannelClusterConfig, error) { return wkdb.EmptyChannelClusterConfig, readErr },
		}
		_, err := r.read(context.Background())
		require.ErrorIs(t, err, readErr)
	}
}
