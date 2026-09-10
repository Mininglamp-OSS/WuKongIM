package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestConfigResponseUsesReceiverRole(t *testing.T) {
	for _, tc := range []struct {
		name               string
		id                 uint64
		previousRole       types.Role
		replicas, learners []uint64
		wantRole           types.Role
	}{
		{"follower", 3, types.RoleFollower, []uint64{1, 2, 3}, []uint64{4}, types.RoleFollower},
		{"learner", 4, types.RoleLearner, []uint64{1, 2, 3}, []uint64{4}, types.RoleLearner},
		{"promoted learner", 4, types.RoleLearner, []uint64{1, 2, 4}, []uint64{3}, types.RoleFollower},
		{"demoted follower", 3, types.RoleFollower, []uint64{1, 2, 4}, []uint64{3}, types.RoleLearner},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader := newTestNode(2, tc.replicas)
			require.NoError(t, leader.Step(types.Event{Type: types.ConfChange, Config: types.Config{
				Replicas: tc.replicas, Learners: tc.learners, Leader: 2, Term: 2, Version: 8, Role: types.RoleLeader,
			}}))
			receiver := newTestNode(tc.id, nil)
			require.NoError(t, receiver.Step(types.Event{Type: types.ConfChange, Config: types.Config{
				Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, Leader: 2, Term: 2, Version: 7, Role: tc.previousRole,
			}}))
			leader.sendPing(tc.id)
			for _, e := range collectEvents(leader) {
				if e.Type == types.Ping {
					require.NoError(t, receiver.Step(e))
				}
			}
			request, ok := findEvent(collectEvents(receiver), types.ConfigReq)
			require.True(t, ok, "the newer configuration should trigger a request")
			require.NoError(t, leader.Step(request))
			response, ok := findEvent(collectEvents(leader), types.ConfigResp)
			require.True(t, ok)
			// Exercise the real leader response, including the legacy sender role.
			wire, err := response.Marshal()
			require.NoError(t, err)
			var decoded types.Event
			require.NoError(t, decoded.Unmarshal(wire))
			require.Equal(t, types.RoleLeader, decoded.Config.Role)
			require.NoError(t, receiver.Step(decoded))
			require.Equal(t, tc.wantRole, receiver.Config().Role)
			require.Equal(t, uint64(2), receiver.LeaderId())
			require.False(t, receiver.IsLeader())
			// The role must select the correct timer and event handlers, not just
			// change a field: the replica must still pull and store the next log.
			for i := 0; i < 30; i++ {
				receiver.Tick()
			}
			sync, ok := findEvent(collectEvents(receiver), types.SyncReq)
			require.True(t, ok, "replica must continue pulling logs")
			require.Equal(t, uint64(2), sync.To)
			require.NoError(t, receiver.Step(types.Event{Type: types.SyncResp, From: 2, To: tc.id, Term: 2,
				Reason: types.ReasonOk, Logs: []types.Log{{Index: 1, Term: 2, Data: []byte("after-config")}}}))
			stored, ok := findEvent(collectEvents(receiver), types.StoreReq)
			require.True(t, ok, "replica must handle the sync response")
			require.Len(t, stored.Logs, 1)
			require.Equal(t, []byte("after-config"), stored.Logs[0].Data)
		})
	}
}

func TestConfigResponseRejectsNonmember(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 2, 2)
	n.cfg.Version = 7
	previous := n.Config().Clone()
	err := n.Step(types.Event{Type: types.ConfigResp, From: 2, Term: 2, Config: types.Config{
		Replicas: []uint64{2, 3, 4}, Leader: 2, Term: 2, Version: 8, Role: types.RoleLeader,
	}})
	require.Error(t, err)
	require.Equal(t, previous, n.Config().Clone(), "an unrelated response must not assign a local role")
}

func TestConfigResponseRejectsOlderVersion(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 2, 2)
	n.cfg.Version = 8
	previous := n.Config().Clone()
	err := n.Step(types.Event{Type: types.ConfigResp, From: 2, Term: 2, Config: types.Config{
		Replicas: []uint64{2, 3}, Learners: []uint64{1}, Leader: 2, Term: 2, Version: 7, Role: types.RoleLeader,
	}})
	require.Error(t, err)
	require.Equal(t, previous, n.Config().Clone())
}
