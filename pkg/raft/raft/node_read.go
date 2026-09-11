package raft

// LeaderReadReady must be called on the owner's event loop, just like Step.
// A leader draining proposals for transfer must not certify a read boundary.
func (n *Node) LeaderReadReady() bool {
	return n.IsLeader() && !n.hasUnpersistedHardState() && !n.stopPropose && !n.truncating
}
