package raft

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/track"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"go.uber.org/zap"
)

func (n *Node) Step(e types.Event) error {
	// Non-voters cannot disrupt an election by supplying a higher term.
	if (e.Type == types.VoteReq || e.Type == types.VoteResp) &&
		(!n.isVoter(n.opts.NodeId) || !n.isVoter(e.From)) {
		return nil
	}

	// n.Info("step event", zap.Uint64("from", e.From), zap.Uint64("to", e.To), zap.Uint32("term", e.Term), zap.Uint64("index", e.Index), zap.String("type", e.Type.String()))
	switch {
	case e.Term == 0: // 本地消息
	case e.Term < n.cfg.Term: // 低于当前任期
		n.Info("received event with lower term", zap.Uint32("term", e.Term), zap.Uint32("currentTerm", n.cfg.Term), zap.Uint64("from", e.From), zap.Uint64("to", e.To), zap.String("type", e.Type.String()))
		return nil
	case e.Term > n.cfg.Term: // 高于当前任期
		if n.cfg.Term > 0 {
			n.Info("received event with higher term", zap.Uint32("term", n.cfg.Term), zap.Uint32("currentTerm", e.Term), zap.Uint64("from", e.From), zap.Uint64("to", e.To), zap.String("type", e.Type.String()))
		}
		if e.Type == types.Ping || e.Type == types.SyncResp {
			if n.cfg.Role == types.RoleLearner {
				n.BecomeLearner(e.Term, e.From)
			} else {
				n.BecomeFollower(e.Term, e.From)
			}
		} else {
			if n.cfg.Role == types.RoleLearner {
				n.Warn("become learner but leader is none", zap.Uint64("from", e.From), zap.String("type", e.Type.String()), zap.Uint32("term", e.Term))
				n.BecomeLearner(e.Term, None)
			} else {
				n.Warn("become follower but leader is none", zap.Uint64("from", e.From), zap.String("type", e.Type.String()), zap.Uint32("term", e.Term))
				n.BecomeFollower(e.Term, None)
			}
		}
	}

	switch e.Type {
	case types.ConfChange: // 配置变更
		return n.switchConfig(e.Config)
	case types.Campaign:
		n.campaign()
	case types.VoteReq: // 投票请求
		if n.cfg.Role != types.RoleLearner {
			if n.canVote(e) {
				if e.From == n.opts.NodeId {
					n.sendVoteResp(types.LocalNode, types.ReasonOk)
				} else {
					n.sendVoteResp(e.From, types.ReasonOk)
				}

				n.voteFor = e.From
				n.electionElapsed = 0
				if e.From != n.opts.NodeId {
					n.Info("agree vote", zap.Uint64("voteFor", e.From), zap.Uint32("term", e.Term), zap.Uint64("index", e.Index))
				}
			} else {
				if n.voteFor != None {
					n.Info("already vote for other", zap.Uint64("voteFor", n.voteFor))
				} else if e.Index < n.queue.lastLogIndex {
					n.Info("lower config version, reject vote")
				} else if e.Term < n.cfg.Term {
					n.Info("lower term, reject vote")
				}
				n.Info("reject vote", zap.Uint64("from", e.From), zap.Uint32("term", e.Term), zap.Uint64("index", e.Index))
				n.sendVoteResp(e.From, types.ReasonError)
			}
		} else {
			n.sendVoteResp(e.From, types.ReasonError)
		}

	case types.ApplyResp: // 应用返回
		if e.Reason == types.ReasonOk {
			n.queue.appliedTo(e.Index)
		} else {
			n.queue.applying = false
		}
	default:
		if n.stepFunc != nil {
			return n.stepFunc(e)
		}

	}
	return nil
}

func (n *Node) stepLeader(e types.Event) error {

	switch e.Type {
	case types.Propose: // 提案
		if n.stopPropose { // 停止提案
			n.Foucs("stop propose", zap.String("key", n.Key()))
			return types.ErrProposalDropped
		}
		n.idleTick = 0
		err := n.queue.append(e.Logs...)
		if err != nil {
			return err
		}
		// if n.Key() == "2&ch1" {
		// 	n.Info("leader propose", zap.Uint64("index", e.Logs[0].Index))
		// }
		n.advance()
	case types.SyncReq: // 同步
		n.idleTick = 0
		isLearner := n.isLearner(e.From) // 当前同步节点是否是学习者
		n.updateSyncInfo(e)              // 更新副本同步信息
		if !isLearner {
			n.updateLeaderCommittedIndex() // 更新领导的提交索引
		}
		// if n.Key() == "40" {
		// 	n.Info("sync...", zap.Uint64("from", e.From), zap.Uint64("index", e.Index), zap.String("reason", e.Reason.String()), zap.Uint64("storedIndex", e.StoredIndex), zap.Uint64("lastLogIndex", n.queue.lastLogIndex))
		// }
		syncInfo := n.replicaSync[e.From]

		// 根据需要切换角色
		n.roleSwitchIfNeed(e)

		// 无数据可同步
		if (e.Reason == types.ReasonOnlySync && e.Index > n.queue.storedIndex) || n.queue.storedIndex == 0 {

			var speed types.Speed
			if n.opts.AutoSuspend {
				syncInfo.emptySyncTick++
				// 超过空同步阈值，挂起
				if syncInfo.emptySyncTick > n.opts.SuspendAfterEmptySyncTick {
					speed = types.SpeedSuspend
				}
			}
			n.sendSyncResp(e.From, e.Index, nil, types.ReasonOk, speed)
			return nil
		}

		syncInfo.emptySyncTick = 0

		if !syncInfo.GetingLogs {
			syncInfo.GetingLogs = true
			n.sendGetLogsReq(e)
			n.advance()
		}

	case types.StoreResp: // 异步存储日志返回
		n.queue.appending = false
		// if n.Key() == "clusterconfig" {
		// 	n.Info("StoreResp...", zap.Uint64("from", e.From), zap.Uint64("index", e.Index))
		// }
		if e.Reason == types.ReasonOk {
			if e.Index > n.queue.lastLogIndex {
				n.Panic("invalid append response", zap.Uint64("index", e.Index), zap.Uint64("lastLogIndex", n.queue.lastLogIndex))
			}
			n.queue.storeTo(e.Index)
			if n.quorum() <= 1 {
				// 如果少于或等于一个节点，那么直接提交
				n.updateLeaderCommittedIndex()
			}
			// 通知副本过来同步日志
			n.sendNotifySync(All)
			n.advance()
		}
	case types.GetLogsResp: // 获取日志返回
		syncInfo := n.replicaSync[e.To]
		if syncInfo != nil {
			syncInfo.GetingLogs = false
		} else {
			n.Error("sync info not found", zap.Uint64("nodeId", e.To))
		}
		for i := range e.Logs {
			e.Logs[i].Record.Add(track.PositionSyncResp)
		}

		n.sendSyncResp(e.To, e.Index, e.Logs, e.Reason, types.SpeedFast)
		n.advance()

		// 角色转换
	case types.LearnerToFollowerResp,
		types.LearnerToLeaderResp,
		types.FollowerToLeaderResp:

		n.stopPropose = false
		syncInfo := n.replicaSync[e.From]
		if syncInfo == nil {
			if n.queue.lastLogIndex > 0 {
				n.Warn("role switch error,syncInfo not exist", zap.Uint64("from", e.From), zap.Uint64("to", e.To), zap.String("type", e.Type.String()))
			}
			return nil
		}
		n.replicaSync[e.From].roleSwitching = false

	case types.ConfigReq: // 配置请求
		n.sendConfigResp(e.From, n.cfg.Clone())
	}

	return nil
}

func (n *Node) stepFollower(e types.Event) error {
	switch e.Type {
	case types.Propose:
		n.Foucs("follower not allow propose", zap.String("key", n.Key()), zap.Int("logs", len(e.Logs)))
	case types.Ping: // 心跳
		n.electionElapsed = 0
		if n.cfg.Leader == None {
			n.BecomeFollower(e.Term, e.From)
		}
		n.updateFollowCommittedIndex(e.CommittedIndex) // 更新提交索引
		n.idleTick = 0
		// 如果领导的配置版本大于本地配置版本，那么请求配置
		if e.ConfigVersion > n.cfg.Version {
			n.sendConfigReq()
		}
	case types.NotifySync:
		// if n.Key() == "2&ch1" {
		// 	n.Info("NotifySync...", zap.Uint64("from", e.From), zap.Uint64("index", e.Index))
		// }
		n.idleTick = 0
		n.suspend = false // 解除挂起
		n.sendSyncReq()
		n.advance()
	case types.SyncResp: // 同步返回
		n.electionElapsed = 0
		n.idleTick = 0
		n.syncRespTimeoutTick = 0
		n.syncing = false
		if !n.onlySync {
			n.onlySync = true
		}

		switch e.Reason {
		case types.ReasonOk:
			// if n.Key() == "clusterconfig" {
			// 	n.Info("SyncResp...", zap.Uint64("from", e.From), zap.Uint64("index", e.Index), zap.Int("len", len(e.Logs)))
			// }
			if len(e.Logs) > 0 {
				err := n.queue.append(e.Logs...)
				if err != nil {
					return err
				}
				// 推进去存储
				n.advance()
			} else {
				if e.Speed == types.SpeedSuspend {
					n.suspend = true
				}
			}
			n.updateFollowCommittedIndex(e.CommittedIndex) // 更新提交索引

		case types.ReasonTruncate:
			// if n.Key() == "clusterconfig" {
			// 	n.Info("truncating...", zap.Bool("truncating", n.truncating), zap.Uint64("from", e.From), zap.Uint64("index", e.Index), zap.Int("len", len(e.Logs)))
			// }
			if n.truncating {
				return nil
			}
			n.truncating = true
			n.sendTruncateReq(e.Index)
			n.advance()
		default:
			n.Error("sync error", zap.Uint64("from", e.From), zap.Uint64("index", e.Index), zap.String("reason", e.Reason.String()))

		}
	case types.TruncateResp: // 裁剪返回
		n.truncating = false
		if e.Reason == types.ReasonOk {
			if e.Index < n.queue.lastLogIndex {
				n.queue.truncateLogTo(e.Index)
			}
			n.sendSyncReq()
			n.advance()
		}
	case types.StoreResp: // 异步存储日志返回
		n.queue.appending = false
		if e.Reason == types.ReasonOk {
			if e.Index > n.queue.lastLogIndex {
				n.Panic("invalid append response", zap.Uint64("index", e.Index), zap.Uint64("lastLogIndex", n.queue.lastLogIndex))
			}
			n.queue.storeTo(e.Index)
			n.sendSyncReq()
			n.advance()
		}

	case types.ConfigResp: // 配置返回
		// 切换配置
		e.Config.Term = n.cfg.Term
		n.switchConfig(e.Config)

	}
	return nil
}

func (n *Node) stepCandidate(e types.Event) error {
	switch e.Type {
	case types.Propose:
		n.Foucs("candidate not allow propose", zap.String("key", n.Key()), zap.Int("logs", len(e.Logs)))
	case types.VoteResp: // 投票返回
		if e.From != n.opts.NodeId {
			n.Info("received vote response", zap.Uint8("reason", e.Reason.Uint8()), zap.Uint64("from", e.From), zap.Uint64("to", e.To), zap.Uint32("term", e.Term), zap.Uint64("index", e.Index))
		}
		n.poll(e)
	}
	return nil
}

func (n *Node) stepLearner(e types.Event) error {
	switch e.Type {
	case types.Propose:
		n.Foucs("learner not allow propose", zap.String("key", n.Key()), zap.Int("logs", len(e.Logs)))
	case types.Ping: // 心跳
		n.electionElapsed = 0
		if n.cfg.Leader == None {
			n.BecomeLearner(e.Term, e.From)
		}
		n.updateFollowCommittedIndex(e.CommittedIndex) // 更新提交索引
		n.idleTick = 0
		// 如果领导的配置版本大于本地配置版本，那么请求配置
		if e.ConfigVersion > n.cfg.Version {
			n.sendConfigReq()
		}
	case types.NotifySync:
		n.idleTick = 0
		n.suspend = false // 解除挂起，允许后续 tickSync
		n.sendSyncReq()
		n.advance()
	case types.SyncResp: // 同步返回
		n.electionElapsed = 0
		n.idleTick = 0
		n.syncRespTimeoutTick = 0
		n.syncing = false
		if !n.onlySync {
			n.onlySync = true
		}

		if e.Reason == types.ReasonOk {
			// if n.Key() == "clusterconfig" {
			// 	n.Info("SyncResp...", zap.Uint64("from", e.From), zap.Uint64("index", e.Index), zap.Int("len", len(e.Logs)))
			// }
			if len(e.Logs) > 0 {
				err := n.queue.append(e.Logs...)
				if err != nil {
					return err
				}
				n.advance()
			} else {
				if e.Speed == types.SpeedSuspend {
					n.suspend = true
				}
			}
			n.updateFollowCommittedIndex(e.CommittedIndex) // 更新提交索引

		} else if e.Reason == types.ReasonTruncate {
			// if n.Key() == "clusterconfig" {
			// 	n.Info("truncating...", zap.Bool("truncating", n.truncating), zap.Uint64("from", e.From), zap.Uint64("index", e.Index), zap.Int("len", len(e.Logs)))
			// }
			if n.truncating {
				return nil
			}
			n.truncating = true
			n.sendTruncateReq(e.Index)
			n.advance()
		} else {
			n.Error("sync error", zap.Uint64("from", e.From), zap.Uint64("index", e.Index), zap.String("reason", e.Reason.String()))
		}
	case types.TruncateResp: // 裁剪返回
		n.truncating = false
		if e.Reason == types.ReasonOk {
			if e.Index < n.queue.lastLogIndex {
				n.queue.truncateLogTo(e.Index)
			}
			n.sendSyncReq()
			n.advance()
		}
	case types.StoreResp: // 异步存储日志返回
		n.queue.appending = false
		if e.Reason == types.ReasonOk {
			if e.Index > n.queue.lastLogIndex {
				n.Panic("invalid append response", zap.Uint64("index", e.Index), zap.Uint64("lastLogIndex", n.queue.lastLogIndex))
			}
			n.queue.storeTo(e.Index)
			n.sendSyncReq()
			n.advance()
		}

	case types.ConfigResp: // 配置返回
		// 切换配置
		e.Config.Term = n.cfg.Term
		n.switchConfig(e.Config)

	}
	return nil
}

// 统计投票
func (n *Node) poll(e types.Event) {
	if n.cfg.Role != types.RoleCandidate || !n.isVoter(n.opts.NodeId) ||
		!n.isVoter(e.From) || e.Term != n.cfg.Term || e.ConfigVersion != n.cfg.Version {
		return
	}
	if _, received := n.votes[e.From]; received {
		return
	}

	n.votes[e.From] = e.Reason == types.ReasonOk
	var granted int
	for id, v := range n.votes {
		if v && n.isVoter(id) {
			granted++
		}
	}
	if len(n.votes) < n.quorum() { // 投票数小于法定数
		return
	}
	if granted >= n.quorum() {
		n.BecomeLeader(n.cfg.Term) // 成为领导者
		n.sendPing(All)            // 成为领导后立马发生ping，开始工作
		n.advance()
	} else {
		n.BecomeFollower(n.cfg.Term, None)
	}
}

// 合法投票数
func (n *Node) quorum() int {
	voters := make(map[uint64]struct{}, len(n.cfg.Replicas))
	for _, id := range n.cfg.Replicas {
		if n.isVoter(id) {
			voters[id] = struct{}{}
		}
	}
	return len(voters)/2 + 1
}

// 是否可以投票
func (n *Node) canVote(e types.Event) bool {
	if !n.isVoter(n.opts.NodeId) || !n.isVoter(e.From) || n.cfg.Role == types.RoleLearner || e.ConfigVersion != n.cfg.Version {
		return false
	}

	// 检查候选人日志信息是否存在
	if len(e.Logs) == 0 {
		n.Info("candidate log info missing, reject vote")
		return false
	}

	if n.cfg.Term > e.Term { // 如果当前任期大于候选人任期，拒绝投票 (不能等于，因为前面becomeFollower的时候，会更新任期 TODO:这里逻辑有点问题，因为n.cfg.Term 永远等于e.Term)
		n.Info("current term is greater than candidate term", zap.Uint32("term", n.cfg.Term), zap.Uint32("candidateTerm", e.Term))
		return false
	}

	if n.voteFor != None && n.voteFor != e.From { // 如果已经投票给其他节点，拒绝投票
		n.Info("already vote for other", zap.Uint64("voteFor", n.voteFor))
		return false
	}

	lastLogIndex := n.queue.lastLogIndex     // 最后一条日志下标
	lastLogTerm := n.lastTermStartIndex.Term // 最后一条日志任期
	candidateLog := e.Logs[0]                // 候选人最后一条日志信息

	// 如果候选人最后一条日志任期小于本地最后一条日志任期，拒绝投票
	// 如果候选人与本节点任期相同，但是候选人最后一条日志下标小于本地最后一条日志下标，拒绝投票
	if candidateLog.Term < lastLogTerm || candidateLog.Term == lastLogTerm && candidateLog.Index < lastLogIndex {
		n.Info("candidate log is not newer", zap.Uint64("lastLogIndex", lastLogIndex), zap.Uint32("lastLogTerm", lastLogTerm), zap.Uint64("candidateIndex", candidateLog.Index), zap.Uint32("candidateTerm", candidateLog.Term))
		return false
	}

	return true
}

// 更新同步信息
func (n *Node) updateSyncInfo(e types.Event) {
	syncInfo := n.replicaSync[e.From]
	if syncInfo == nil {
		syncInfo = &SyncInfo{}
		n.replicaSync[e.From] = syncInfo
	}
	syncInfo.SyncTick = 0
	syncInfo.LastSyncIndex = e.Index
	syncInfo.StoredIndex = e.StoredIndex
}

// 更新领导的提交索引
func (n *Node) updateLeaderCommittedIndex() bool {
	newCommitted := n.committedIndexForLeader() // 通过副本同步信息计算领导已提交下标
	updated := false
	if newCommitted > n.queue.committedIndex {
		n.queue.committedIndex = newCommitted
		updated = true
		n.Debug("update leader committed index", zap.Uint64("lastIndex", n.queue.lastLogIndex), zap.Uint32("term", n.cfg.Term), zap.Uint64("committedIndex", n.queue.committedIndex))
	}
	return updated
}

// 通过副本同步信息计算已提交下标
func (n *Node) committedIndexForLeader() uint64 {

	committed := n.queue.committedIndex
	quorum := n.quorum() // r.replicas 不包含本节点
	if quorum <= 1 {     // 如果少于或等于一个节点，那么直接返回最后一条日志下标
		return n.queue.storedIndex
	}

	// 获取比指定参数小的最大日志下标
	getMaxLogIndexLessThanParam := func(maxIndex uint64) uint64 {
		secondMaxIndex := uint64(0)
		for id, syncInfo := range n.replicaSync {
			if id == n.opts.NodeId || !n.isVoter(id) {
				continue
			}
			if syncInfo.StoredIndex < maxIndex || maxIndex == 0 {
				if secondMaxIndex < syncInfo.StoredIndex {
					secondMaxIndex = syncInfo.StoredIndex
				}
			}
		}
		if secondMaxIndex > 0 {
			return secondMaxIndex - 1
		}
		return secondMaxIndex
	}

	maxLogIndex := uint64(0)
	newCommitted := uint64(0)
	for {
		count := 0
		maxLogIndex = getMaxLogIndexLessThanParam(maxLogIndex)
		if maxLogIndex == 0 {
			break
		}
		if maxLogIndex <= committed {
			break
		}
		if maxLogIndex > n.queue.storedIndex {
			continue
		}
		for id, syncInfo := range n.replicaSync {
			if id == n.opts.NodeId || !n.isVoter(id) {
				continue
			}
			if syncInfo.StoredIndex >= maxLogIndex+1 {
				count++
			}
			if count+1 >= quorum {
				newCommitted = maxLogIndex
				break
			}
		}
	}
	if newCommitted > committed {
		return min(newCommitted, n.queue.storedIndex)
	}
	return committed

}

// 更新跟随者的提交索引
func (n *Node) updateFollowCommittedIndex(leaderCommittedIndex uint64) {
	if leaderCommittedIndex == 0 || leaderCommittedIndex <= n.queue.committedIndex {
		return
	}
	newCommittedIndex := n.committedIndexForFollow(leaderCommittedIndex)
	if newCommittedIndex > n.queue.committedIndex {
		n.queue.committedIndex = newCommittedIndex
		n.Debug("update follow committed index", zap.Uint64("nodeId", n.opts.NodeId), zap.Uint32("term", n.cfg.Term), zap.Uint64("committedIndex", n.queue.committedIndex))
	}
}

// 获取跟随者的提交索引
func (n *Node) committedIndexForFollow(leaderCommittedIndex uint64) uint64 {
	if leaderCommittedIndex > n.queue.committedIndex {
		return min(leaderCommittedIndex, n.queue.storedIndex)

	}
	return n.queue.committedIndex
}

func (n *Node) roleSwitchIfNeed(e types.Event) {
	if !n.isLearner(e.From) && !n.isVoter(e.From) {
		return
	}
	// Sync indices include received-but-not-stored logs. Promotion needs the
	// committed prefix on disk; leadership transfer needs the entire tail.
	if n.isLearner(e.From) && e.StoredIndex <= n.queue.committedIndex {
		return
	}

	// 没有需要切换的配置
	if n.cfg.MigrateTo == 0 || n.cfg.MigrateFrom == 0 {
		// 兜底：处理 orphan learner —— 在 Learners 列表中但前次迁移已完成
		// （MigrateFrom/MigrateTo 已清零）。这种 learner 没有任何迁移标记可
		// 触发晋升，会永远卡在 learner 角色。在这里检测它并在日志追上后
		// 主动晋升为 follower。
		//
		// 注意：外层条件用的是 ||，所以当只清零了一半（例如 MigrateFrom!=0
		// 而 MigrateTo==0）时也会进入这里。这种半清零状态属于迁移进行中，
		// 不应触发 orphan 晋升，因此内层必须同时校验两者都为 0。
		if n.isLearner(e.From) && n.cfg.MigrateTo == 0 && n.cfg.MigrateFrom == 0 {
			syncInfo := n.replicaSync[e.From]
			if syncInfo == nil || syncInfo.roleSwitching {
				return
			}
			// 与正常 learner→follower 分支保持一致：单副本集群必须完全追上
			// 领导者的日志，否则用 gap-based 判定。
			if len(n.cfg.Replicas) == 1 {
				if e.Index > n.queue.lastLogIndex && e.StoredIndex > n.queue.lastLogIndex {
					syncInfo.roleSwitching = true
					n.sendLearnerToFollowerReq(e.From)
				}
			} else if e.Index+n.opts.LearnerToFollowerMinLogGap > n.queue.lastLogIndex {
				syncInfo.roleSwitching = true
				n.sendLearnerToFollowerReq(e.From)
			}
		}
		return
	}

	if n.cfg.MigrateTo != e.From { // 迁移目标节点不是当前同步节点，才进行进行角色切换
		return
	}

	syncInfo := n.replicaSync[e.From]
	if syncInfo.roleSwitching {
		return
	}

	isLearner := n.isLearner(e.From) // 当前同步节点是否是学习者

	if isLearner {
		// 如果迁移的源节点是领导者，那么学习者必须完全追上领导者的日志
		if n.cfg.MigrateFrom == n.cfg.Leader { // 学习者转领导者
			if e.Index >= n.queue.lastLogIndex+1 && e.StoredIndex >= n.queue.lastLogIndex+1 {
				syncInfo.roleSwitching = true // 学习者转让中
				// 发送学习者转为领导者
				n.sendLearnerToLeaderReq(e.From)
			} else if e.Index+n.opts.LearnerToLeaderMinLogGap > n.queue.lastLogIndex { // 如果日志差距达到预期，则当前领导停止接受任何提案，等待学习者日志完全追赶上
				n.stopPropose = true // 停止提案
			}

		} else { // 学习者转追随者
			// 如果learner的日志已经追上了follower的日志，那么将learner转为follower
			if len(n.cfg.Replicas) == 1 {
				if e.Index > n.queue.lastLogIndex && e.StoredIndex > n.queue.lastLogIndex {
					syncInfo.roleSwitching = true
					n.sendLearnerToFollowerReq(e.From)
				}
			} else {
				if e.Index+n.opts.LearnerToFollowerMinLogGap > n.queue.lastLogIndex {
					syncInfo.roleSwitching = true
					// 发送学习者转为追随者
					n.sendLearnerToFollowerReq(e.From)
				}
			}
		}
	} else if n.cfg.MigrateFrom == n.cfg.Leader && n.cfg.MigrateTo == e.From { // // 追随者转为领导者
		if e.Index >= n.queue.lastLogIndex+1 && e.StoredIndex >= n.queue.lastLogIndex+1 {
			syncInfo.roleSwitching = true
			// 发送追随者转为领导者
			n.sendFollowToLeaderReq(e.From)
		} else if e.Index+n.opts.FollowerToLeaderMinLogGap > n.queue.lastLogIndex { // 如果日志差距达到预期，则当前领导停止接受任何提案，等待学习者日志完全追赶上
			n.stopPropose = true // 停止提案
		}
	}
}
