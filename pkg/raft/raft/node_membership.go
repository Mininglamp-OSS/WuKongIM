package raft

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

func isVoter(cfg types.Config, id uint64) bool {
	if id == None {
		return false
	}
	for _, learner := range cfg.Learners {
		if learner == id {
			return false
		}
	}
	for _, replica := range cfg.Replicas {
		if replica == id {
			return true
		}
	}
	return false
}

func (n *Node) isVoter(id uint64) bool { return isVoter(n.cfg, id) }

func sameVoters(a, b types.Config) bool {
	for _, id := range a.Replicas {
		if isVoter(a, id) != isVoter(b, id) {
			return false
		}
	}
	for _, id := range b.Replicas {
		if isVoter(a, id) != isVoter(b, id) {
			return false
		}
	}
	return true
}
