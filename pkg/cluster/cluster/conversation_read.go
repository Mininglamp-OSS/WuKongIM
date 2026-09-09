package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

var ErrConversationReadRetry = errors.New("retry required")

const (
	conversationConfigPath   = "/rpc/channel/conversationConfig/v1"
	conversationBoundaryPath = "/rpc/channel/conversationBoundary/v1"
)

type conversationReadRequest struct {
	ChannelID   string                    `json:"channel_id"`
	ChannelType uint8                     `json:"channel_type"`
	Expected    wkdb.ChannelClusterConfig `json:"expected"`
	Budget      time.Duration             `json:"budget"`
}

// Version and required payload fields prevent an old/malformed success response
// from being mistaken for an authoritative zero. Existing RPCs stay unchanged.
type conversationReadResponse struct {
	Version  int                        `json:"version"`
	Config   *wkdb.ChannelClusterConfig `json:"config,omitempty"`
	Sequence *uint64                    `json:"sequence,omitempty"`
}

type conversationReader struct {
	nodeID uint64
	load   func(context.Context, string, uint8) (wkdb.ChannelClusterConfig, error)
	state  func(context.Context, string, uint8) (raftgroup.ReadState, error)
	local  func(string, uint8) (uint64, uint64, error)
	remote func(context.Context, uint64, string, conversationReadRequest) (conversationReadResponse, error)
}

func (s *Server) conversationReader() conversationReader {
	return conversationReader{
		nodeID: s.opts.ConfigOptions.NodeId,
		load:   s.loadConversationConfig,
		state:  s.channelServer.ReadLeaderState,
		local:  s.db.GetChannelLastMessageSeq,
		remote: s.requestConversationRead,
	}
}

// GetChannelLastMessageSeq resolves the channel's message owner independently
// of the UID slot owning the conversation. The complete read has one budget and
// at most one retry, always reloading metadata instead of using a local fallback.
func (s *Server) GetChannelLastMessageSeq(ctx context.Context, channelID string, channelType uint8) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.conversationReadTimeout())
	defer cancel()
	return s.conversationReader().latest(ctx, channelID, channelType)
}

func (r conversationReader) latest(ctx context.Context, id string, typ uint8) (uint64, error) {
	seenConfig := false
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		// Leave time to refresh the route even when the first peer times out.
		attemptCtx := ctx
		cancel := func() {}
		if deadline, ok := ctx.Deadline(); ok {
			attemptCtx, cancel = context.WithTimeout(ctx, time.Until(deadline)/time.Duration(2-attempt))
		}
		seq, found, err := r.attempt(attemptCtx, id, typ, !seenConfig)
		cancel()
		seenConfig = seenConfig || found
		if err == nil {
			return seq, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return 0, ErrConversationReadRetry
}

func (r conversationReader) attempt(ctx context.Context, id string, typ uint8, allowMissing bool) (uint64, bool, error) {
	cfg, err := r.load(ctx, id, typ)
	if errors.Is(err, wkdb.ErrNotFound) && allowMissing {
		// Preserve v2.2.5's unused-channel semantics without creating metadata.
		return 0, false, ctx.Err()
	}
	if err != nil {
		return 0, false, err
	}
	if !validConversationConfig(cfg, id, typ) {
		return 0, true, ErrConversationReadRetry
	}
	var seq uint64
	if cfg.LeaderId == r.nodeID {
		seq, err = r.readLocal(ctx, cfg)
	} else {
		var resp conversationReadResponse
		resp, err = r.remote(ctx, cfg.LeaderId, conversationBoundaryPath,
			conversationReadRequest{ChannelID: id, ChannelType: typ, Expected: cfg})
		if err == nil {
			if resp.Version != 1 || resp.Sequence == nil || resp.Config == nil || !cfg.Equal(*resp.Config) {
				err = ErrConversationReadRetry
			} else {
				seq = *resp.Sequence
			}
		}
	}
	if err != nil {
		return 0, true, err
	}
	after, err := r.load(ctx, id, typ)
	if err != nil {
		return 0, true, err
	}
	if !cfg.Equal(after) {
		return 0, true, ErrConversationReadRetry
	}
	return seq, true, ctx.Err()
}

func validConversationConfig(cfg wkdb.ChannelClusterConfig, id string, typ uint8) bool {
	return cfg.ChannelId == id && cfg.ChannelType == typ && cfg.LeaderId != 0 && cfg.Term != 0 &&
		cfg.Status == wkdb.ChannelClusterStatusNormal &&
		wkutil.ArrayContainsUint64(cfg.Replicas, cfg.LeaderId) &&
		!wkutil.ArrayContainsUint64(cfg.Learners, cfg.LeaderId)
}

func (r conversationReader) readLocal(ctx context.Context, expected wkdb.ChannelClusterConfig) (uint64, error) {
	if !validConversationConfig(expected, expected.ChannelId, expected.ChannelType) || expected.LeaderId != r.nodeID {
		return 0, ErrConversationReadRetry
	}
	id, typ := expected.ChannelId, expected.ChannelType
	cfg, err := r.load(ctx, id, typ)
	if err != nil {
		return 0, err
	}
	if !cfg.Equal(expected) {
		return 0, ErrConversationReadRetry
	}
	before, err := r.state(ctx, id, typ)
	if err != nil {
		return 0, err
	}
	if !conversationStateReady(before, cfg) {
		return 0, ErrConversationReadRetry
	}
	seq, _, err := r.local(id, typ)
	if err != nil {
		return 0, err
	}
	after, err := r.state(ctx, id, typ)
	if err != nil {
		return 0, err
	}
	if before.Exists != after.Exists || !conversationStateReady(after, cfg) {
		return 0, ErrConversationReadRetry
	}
	latest, err := r.load(ctx, id, typ)
	if err != nil {
		return 0, err
	}
	if !cfg.Equal(latest) {
		return 0, ErrConversationReadRetry
	}
	return seq, ctx.Err()
}

func conversationStateReady(state raftgroup.ReadState, cfg wkdb.ChannelClusterConfig) bool {
	if !state.Exists {
		// Idle channels are automatically destroyed in this architecture. Reading
		// their designated owner's durable tail must not wake them. A dormant
		// channel in transfer cannot establish that it finished draining.
		return cfg.MigrateFrom == 0 && cfg.MigrateTo == 0
	}
	return state.Ready && state.LeaderID == cfg.LeaderId && state.Term == cfg.Term && state.ConfigVersion == cfg.ConfVersion
}

func (s *Server) conversationReadTimeout() time.Duration {
	if s.opts.ReqTimeout > 0 {
		return s.opts.ReqTimeout
	}
	return 5 * time.Second
}

func (s *Server) requestConversationRead(ctx context.Context, nodeID uint64, path string, req conversationReadRequest) (conversationReadResponse, error) {
	if err := ctx.Err(); err != nil {
		return conversationReadResponse{}, err
	}
	req.Budget = s.conversationReadTimeout()
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < req.Budget {
		req.Budget = time.Until(deadline)
	}
	data, err := json.Marshal(req)
	if err != nil {
		return conversationReadResponse{}, err
	}
	resp, err := s.RequestWithContext(ctx, nodeID, path, data)
	if err != nil {
		return conversationReadResponse{}, err
	}
	if resp == nil || resp.Status != proto.StatusOK {
		return conversationReadResponse{}, ErrConversationReadRetry
	}
	var result conversationReadResponse
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return result, err
	}
	if result.Version != 1 {
		return result, ErrConversationReadRetry
	}
	return result, ctx.Err()
}

func (s *Server) loadConversationConfig(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
	if err := ctx.Err(); err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	leader, err := s.SlotLeaderIdOfChannel(id, typ)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	if leader == s.opts.ConfigOptions.NodeId {
		return s.loadConversationConfigLocal(ctx, id, typ)
	}
	resp, err := s.requestConversationRead(ctx, leader, conversationConfigPath, conversationReadRequest{ChannelID: id, ChannelType: typ})
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	if resp.Config == nil {
		return wkdb.EmptyChannelClusterConfig, ErrConversationReadRetry
	}
	if wkdb.IsEmptyChannelClusterConfig(*resp.Config) {
		return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
	}
	if resp.Config.ChannelId != id || resp.Config.ChannelType != typ {
		return wkdb.EmptyChannelClusterConfig, ErrConversationReadRetry
	}
	return *resp.Config, nil
}

func (s *Server) loadConversationConfigLocal(ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
	slotID := s.getSlotId(id)
	before, err := s.slotServer.ReadLeaderState(ctx, slotID)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	if !before.Exists || !before.Ready || before.LeaderID != s.opts.ConfigOptions.NodeId || before.AppliedIndex < before.CommittedIndex || s.cfgServer.SlotLeaderId(slotID) != before.LeaderID {
		return wkdb.EmptyChannelClusterConfig, ErrConversationReadRetry
	}
	cfg, readErr := s.db.GetChannelClusterConfig(id, typ)
	after, err := s.slotServer.ReadLeaderState(ctx, slotID)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	// Also reject metadata written/applied concurrently with this DB read.
	if before != after || s.cfgServer.SlotLeaderId(slotID) != before.LeaderID {
		return wkdb.EmptyChannelClusterConfig, ErrConversationReadRetry
	}
	if err := ctx.Err(); err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	return cfg, readErr
}

func (r *rpcServer) handleConversationRead(c *wkserver.Context, configOnly bool) {
	var req conversationReadRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil || req.ChannelID == "" || req.ChannelType == 0 || req.Budget <= 0 {
		c.WriteErr(ErrConversationReadRetry)
		return
	}
	budget := r.s.conversationReadTimeout()
	if req.Budget < budget {
		budget = req.Budget
	}
	ctx, cancel := context.WithTimeout(r.s.cancelCtx, budget)
	defer cancel()
	resp := conversationReadResponse{Version: 1}
	if configOnly {
		cfg, err := r.s.loadConversationConfigLocal(ctx, req.ChannelID, req.ChannelType)
		if err != nil && !errors.Is(err, wkdb.ErrNotFound) {
			c.WriteErr(ErrConversationReadRetry)
			return
		}
		resp.Config = &cfg
	} else {
		if req.Expected.ChannelId != req.ChannelID || req.Expected.ChannelType != req.ChannelType {
			c.WriteErr(ErrConversationReadRetry)
			return
		}
		seq, err := r.s.conversationReader().readLocal(ctx, req.Expected)
		if err != nil {
			c.WriteErr(ErrConversationReadRetry)
			return
		}
		resp.Config, resp.Sequence = &req.Expected, &seq
	}
	data, err := json.Marshal(resp)
	if err != nil {
		c.WriteErr(ErrConversationReadRetry)
		return
	}
	c.Write(data)
}
