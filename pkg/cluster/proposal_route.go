package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

// resolveProposalLeader separates cluster-wide write routing from local runtime
// ownership checks. Observations are hints only: the target Raft runtime still
// validates every proposal. A rejected hint is bypassed on the next attempt.
func (c *Cluster) resolveProposalLeader(ctx context.Context, slotID multiraft.SlotID, refresh bool) (multiraft.NodeID, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	leader, err := c.router.LeaderOf(slotID)
	if err == nil && (!refresh || c.IsLocal(leader)) {
		return leader, nil
	}
	if err != nil && !errors.Is(err, ErrSlotNotFound) && !errors.Is(err, ErrNoLeader) {
		return 0, err
	}
	peers := c.PeersForSlot(slotID)
	if len(peers) == 0 {
		return 0, ErrSlotNotFound
	}
	if !refresh {
		if leader := c.observedProposalLeader(slotID, peers); leader != 0 {
			return leader, nil
		}
	}

	// Probe only this slot's replicas, not every slot or the controller. Divide
	// the remaining discovery budget so an unavailable peer cannot consume it all.
	visited := make(map[multiraft.NodeID]bool, len(peers))
	targets := append([]multiraft.NodeID(nil), peers...)
	var lastErr error
	for len(targets) > 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		target := targets[0]
		targets = targets[1:]
		if target == 0 || visited[target] {
			continue
		}
		visited[target] = true
		probeBudget := c.timeoutConfig().ForwardRetryBudget
		if deadline, ok := ctx.Deadline(); ok {
			probeBudget = time.Until(deadline)
		}
		probeBudget /= time.Duration(len(peers) - len(visited) + 1)
		probeCtx, cancel := context.WithTimeout(ctx, probeBudget)
		status, err := c.managedSlots().statusOnNode(probeCtx, target, slotID)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		leader := status.LeaderID
		if leader == 0 || !nodeIDsContain(peers, leader) || !nodeIDsContain(status.CurrentVoters, leader) {
			continue
		}
		// Confirm the leader on the node itself; a follower may retain an old
		// leader observation. Never substitute DesiredPeers[0]/PreferredLeader.
		if leader == target {
			return leader, nil
		}
		if !visited[leader] {
			targets = append([]multiraft.NodeID{leader}, targets...)
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if lastErr != nil {
		return 0, fmt.Errorf("resolve slot %d leader: %w", slotID, errors.Join(ErrNoLeader, lastErr))
	}
	return 0, ErrNoLeader
}

// observedProposalLeader uses the existing point-indexed observation cache as
// a fast routing hint. It neither performs controller RPC nor scans all slots.
func (c *Cluster) observedProposalLeader(slotID multiraft.SlotID, peers []multiraft.NodeID) multiraft.NodeID {
	a := c.agent
	if a == nil {
		return 0
	}
	a.observationMu.RLock()
	view, ok := a.observationState.RuntimeViews[uint32(slotID)]
	generation := a.observationState.LeaderGeneration
	controllerLeader := a.observationState.LeaderID
	a.observationMu.RUnlock()
	knownControllerLeader := c.ControllerLeaderID()
	if !ok || view.SlotID != uint32(slotID) || generation == 0 || controllerLeader == 0 ||
		(knownControllerLeader != 0 && controllerLeader != knownControllerLeader) {
		return 0
	}
	leader := multiraft.NodeID(view.LeaderID)
	if leader == 0 || !nodeIDsContain(peers, leader) || !assignmentContainsPeer(view.CurrentVoters, view.LeaderID) {
		return 0
	}
	age := time.Since(view.LastReportAt)
	if view.LastReportAt.IsZero() || age < 0 || age > c.timeoutConfig().ObservationRuntimeFullSyncInterval {
		return 0
	}
	if epoch, ok := c.assignments.ConfigEpochForSlot(slotID); ok && view.ObservedConfigEpoch < epoch {
		return 0
	}
	return leader
}
