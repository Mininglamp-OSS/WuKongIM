package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/assert"
)

// newTestRaftWithNode wires a minimal *Raft around an existing *Node so we
// can exercise unexported helpers like learnTo without standing up the full
// Storage/Transport/loop machinery.
func newTestRaftWithNode(n *Node) *Raft {
	return &Raft{
		node: n,
		Log:  wklog.NewWKLog("test-raft"),
	}
}

// TestLearnTo_OrphanLearner_Promotes verifies the orphan-learner path: when
// MigrateFrom/MigrateTo are both zero but learnerId still sits in
// cfg.Learners, learnTo should return a config that moves the node out of
// Learners and into Replicas without touching Leader.
func TestLearnTo_OrphanLearner_Promotes(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(n, 3)
	n.cfg.Learners = []uint64{4}
	n.cfg.MigrateTo = 0
	n.cfg.MigrateFrom = 0
	r := newTestRaftWithNode(n)

	cfg, err := r.learnTo(4)
	assert.NoError(t, err)
	assert.False(t, wkutil.ArrayContainsUint64(cfg.Learners, 4), "learner should be removed from Learners")
	assert.True(t, wkutil.ArrayContainsUint64(cfg.Replicas, 4), "learner should be added to Replicas")
	assert.Equal(t, uint64(0), cfg.MigrateFrom)
	assert.Equal(t, uint64(0), cfg.MigrateTo)
	// orphan path must NOT mutate the leader: the original leader (node 1)
	// is still in Replicas, so no leadership transfer happens.
	assert.Equal(t, uint64(1), cfg.Leader)
	// original replicas must be preserved.
	assert.True(t, wkutil.ArrayContainsUint64(cfg.Replicas, 1))
	assert.True(t, wkutil.ArrayContainsUint64(cfg.Replicas, 2))
	assert.True(t, wkutil.ArrayContainsUint64(cfg.Replicas, 3))
}

// TestLearnTo_OrphanLearner_AlreadyInReplicas_NoDuplicate guards against the
// orphan path accidentally re-appending an id that already sits in Replicas.
func TestLearnTo_OrphanLearner_AlreadyInReplicas_NoDuplicate(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3, 4})
	makeLeader(n, 3)
	n.cfg.Learners = []uint64{4}
	n.cfg.MigrateTo = 0
	n.cfg.MigrateFrom = 0
	r := newTestRaftWithNode(n)

	cfg, err := r.learnTo(4)
	assert.NoError(t, err)
	count := 0
	for _, id := range cfg.Replicas {
		if id == 4 {
			count++
		}
	}
	assert.Equal(t, 1, count, "Replicas should contain learnerId exactly once")
}

// TestLearnTo_OrphanLearner_NotInLearners_Rejected verifies that the orphan
// path only fires when learnerId is actually present in cfg.Learners. A
// random id that is neither in Learners nor matches MigrateTo must be
// rejected, otherwise the orphan branch becomes an unrestricted "promote
// anyone to follower" hole.
func TestLearnTo_OrphanLearner_NotInLearners_Rejected(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(n, 3)
	n.cfg.Learners = []uint64{4}
	n.cfg.MigrateTo = 0
	n.cfg.MigrateFrom = 0
	r := newTestRaftWithNode(n)

	// 5 is not in Learners and does not match MigrateTo.
	_, err := r.learnTo(5)
	assert.Error(t, err)
}

// TestLearnTo_NormalMigration_Unchanged guards against the orphan branch
// regressing the existing migration path: when MigrateTo/MigrateFrom are
// set, behavior must match the pre-fix implementation (learnerId promoted
// into Replicas, MigrateFrom removed, fields cleared).
func TestLearnTo_NormalMigration_Unchanged(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(n, 3)
	n.cfg.Learners = []uint64{4}
	n.cfg.MigrateFrom = 2
	n.cfg.MigrateTo = 4
	r := newTestRaftWithNode(n)

	cfg, err := r.learnTo(4)
	assert.NoError(t, err)
	assert.False(t, wkutil.ArrayContainsUint64(cfg.Learners, 4))
	assert.True(t, wkutil.ArrayContainsUint64(cfg.Replicas, 4))
	assert.False(t, wkutil.ArrayContainsUint64(cfg.Replicas, 2), "MigrateFrom node should be removed from Replicas")
	assert.Equal(t, uint64(0), cfg.MigrateFrom)
	assert.Equal(t, uint64(0), cfg.MigrateTo)
}

// TestLearnTo_NormalMigration_LeaderHandoff verifies the learner→leader
// path is preserved: when MigrateFrom is the current leader, the new Leader
// in the returned config must become MigrateTo.
func TestLearnTo_NormalMigration_LeaderHandoff(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(n, 3)
	n.cfg.Learners = []uint64{4}
	n.cfg.MigrateFrom = 1 // current leader
	n.cfg.MigrateTo = 4
	r := newTestRaftWithNode(n)

	cfg, err := r.learnTo(4)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), cfg.Leader, "leader should hand off to MigrateTo")
	assert.False(t, wkutil.ArrayContainsUint64(cfg.Replicas, 1), "old leader (MigrateFrom) should be removed from Replicas")
	assert.True(t, wkutil.ArrayContainsUint64(cfg.Replicas, 4))
}

// TestLearnTo_NormalMigration_WrongLearnerId_Rejected ensures the
// pre-existing guard still rejects a learnerId that does not match
// MigrateTo when a migration is active.
func TestLearnTo_NormalMigration_WrongLearnerId_Rejected(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(n, 3)
	n.cfg.Learners = []uint64{4}
	n.cfg.MigrateFrom = 2
	n.cfg.MigrateTo = 4
	r := newTestRaftWithNode(n)

	// learnerId 5 != MigrateTo 4 and we are mid-migration, so reject.
	_, err := r.learnTo(5)
	assert.Error(t, err)
}

// ==================== roleSwitchIfNeed half-cleared regression ====================

// TestRoleSwitchIfNeed_HalfClearedMigration_NoOrphanFire verifies P2-2:
// when only one of MigrateFrom/MigrateTo is zero (the migration is still
// in flight), the orphan fallback must NOT fire even if e.From happens to
// be a learner. Only the fully cleared (both zero) state qualifies.
func TestRoleSwitchIfNeed_HalfClearedMigration_NoOrphanFire(t *testing.T) {
	cases := []struct {
		name        string
		migrateFrom uint64
		migrateTo   uint64
	}{
		{"MigrateFrom set, MigrateTo zero", 2, 0},
		{"MigrateFrom zero, MigrateTo set", 0, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2, 3})
			makeLeader(n, 3)
			n.cfg.Learners = []uint64{4}
			n.cfg.MigrateFrom = tc.migrateFrom
			n.cfg.MigrateTo = tc.migrateTo
			n.queue.lastLogIndex = 10
			n.opts.LearnerToFollowerMinLogGap = 100
			n.replicaSync[4] = &SyncInfo{}

			// e.From=4 is a learner and caught up; orphan would have fired.
			n.roleSwitchIfNeed(types.Event{From: 4, Index: 5, StoredIndex: 5})

			assert.False(t, n.replicaSync[4].roleSwitching, "half-cleared migration must not trigger orphan promotion")
			events := collectEvents(n)
			assert.Equal(t, 0, countEvents(events, types.LearnerToFollowerReq))
		})
	}
}

// TestRoleSwitchIfNeed_OrphanLearner_SingleReplica_StrictCatchUp verifies
// P2-1: in a single-replica cluster, the orphan branch (like the normal
// learner→follower branch) must require the learner to fully catch up
// (e.Index > lastLogIndex) before promotion, not just be within the gap.
func TestRoleSwitchIfNeed_OrphanLearner_SingleReplica_StrictCatchUp(t *testing.T) {
	// gap-close but not strictly caught up → no promotion.
	n := newTestNode(1, []uint64{1})
	makeLeader(n, 3)
	n.cfg.Learners = []uint64{4}
	n.cfg.MigrateTo = 0
	n.cfg.MigrateFrom = 0
	n.queue.lastLogIndex = 10
	n.opts.LearnerToFollowerMinLogGap = 100
	n.replicaSync[4] = &SyncInfo{}

	// Index=5, lastLogIndex=10 → gap path would fire, strict path must not.
	n.roleSwitchIfNeed(types.Event{From: 4, Index: 5, StoredIndex: 5})
	assert.False(t, n.replicaSync[4].roleSwitching, "single-replica orphan must require strict catch-up")
	events := collectEvents(n)
	assert.Equal(t, 0, countEvents(events, types.LearnerToFollowerReq))

	// strictly caught up → promotion fires.
	n.roleSwitchIfNeed(types.Event{From: 4, Index: 11, StoredIndex: 11})
	assert.True(t, n.replicaSync[4].roleSwitching, "single-replica orphan should promote when fully caught up")
	events = collectEvents(n)
	_, ok := findEvent(events, types.LearnerToFollowerReq)
	assert.True(t, ok)
}
