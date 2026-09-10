package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
)

// Separate the UID owner from the channel message owner. The real handlers and
// Store command codecs below must never use the UID owner's local message tail.
type conversationBoundaryCluster struct {
	icluster.ICluster
	seq   uint64
	err   error
	delay bool
}

func (c *conversationBoundaryCluster) SlotLeaderOfChannel(string, uint8) (*clustertypes.Node, error) {
	return &clustertypes.Node{Id: 1001}, nil
}
func (c *conversationBoundaryCluster) GetChannelLastMessageSeq(ctx context.Context, id string, typ uint8) (uint64, error) {
	expected := "bob"
	if typ == 1 {
		expected = options.GetFakeChannelIDWith("alice", "bob")
	}
	if id != expected {
		return 0, fmt.Errorf("unexpected channel %q", id)
	}
	if c.delay {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return c.seq, c.err
}

type conversationBoundaryDB struct {
	wkdb.DB
	localSeq uint64
	readTo   uint64
	reads    int
}

func (d *conversationBoundaryDB) GetChannelLastMessageSeq(string, uint8) (uint64, uint64, error) {
	d.reads++
	return d.localSeq, 0, nil
}
func (d *conversationBoundaryDB) GetConversation(uid, channel string, typ uint8) (wkdb.Conversation, error) {
	return wkdb.Conversation{Id: 1, Uid: uid, ChannelId: channel, ChannelType: typ, Type: wkdb.ConversationTypeChat, ReadToMsgSeq: d.readTo}, nil
}

type conversationBoundarySlot struct {
	icluster.Slot
	boundaries []uint64
}

func (s *conversationBoundarySlot) GetSlotId(string) uint32 { return 7 }
func (s *conversationBoundarySlot) ProposeUntilApplied(_ uint32, data []byte) (*rafttypes.ProposeResp, error) {
	cmd := &store.CMD{}
	if err := cmd.Unmarshal(data); err != nil {
		return nil, err
	}
	switch cmd.CmdType {
	case store.CMDAddOrUpdateUserConversations:
		_, conversations, err := cmd.DecodeCMDAddOrUpdateUserConversations()
		if err != nil {
			return nil, err
		}
		for _, c := range conversations {
			s.boundaries = append(s.boundaries, c.ReadToMsgSeq)
		}
	case store.CMDUpdateConversationDeletedAtMsgSeq:
		_, _, _, seq, err := cmd.DecodeCMDUpdateConversationDeletedAtMsgSeq()
		if err != nil {
			return nil, err
		}
		s.boundaries = append(s.boundaries, seq)
	default:
		return nil, fmt.Errorf("unexpected command %s", cmd.CmdType)
	}
	return &rafttypes.ProposeResp{}, nil
}

func conversationBoundaryRequest(t *testing.T, path string, typ uint8, unread int, localSeq, remoteSeq uint64, remoteStatus int, readTo ...uint64) (int, []uint64, int) {
	t.Helper()
	oldOptions, oldCluster, oldStore := options.G, service.Cluster, service.Store
	defer func() { options.G, service.Cluster, service.Store = oldOptions, oldCluster, oldStore }()
	options.G = options.New()
	options.G.Cluster.NodeId = 1001
	cluster := &conversationBoundaryCluster{seq: remoteSeq}
	if remoteStatus == 503 {
		cluster.err = errors.New("private storage path must not leak")
	}
	if remoteStatus == 504 {
		cluster.delay = true
	}
	options.G.Cluster.ReqTimeout = 20 * time.Millisecond
	service.Cluster = cluster
	db := &conversationBoundaryDB{localSeq: localSeq}
	if len(readTo) > 0 {
		db.readTo = readTo[0]
	}
	slot := &conversationBoundarySlot{}
	service.Store = store.New(store.NewOptions(store.WithDB(db), store.WithSlot(slot)))
	router := wkhttp.New()
	newConversation(nil).route(router)
	body := fmt.Sprintf(`{"uid":"alice","channel_id":"bob","channel_type":%d,"unread":%d}`, typ, unread)
	req := httptest.NewRequest(http.MethodPost, "/conversations/"+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if remoteStatus == 499 {
		ctx, cancel := context.WithCancel(req.Context())
		cancel()
		req = req.WithContext(ctx)
	}
	response := httptest.NewRecorder()
	router.GetGinRoute().ServeHTTP(response, req)
	t.Logf("path=%s type=%d local_seq=%d remote_seq=%d remote_status=%d http=%d writes=%v local_reads=%d", path, typ, localSeq, remoteSeq, remoteStatus, response.Code, slot.boundaries, db.reads)
	if response.Code == 503 && strings.TrimSpace(response.Body.String()) != `{"msg":"retry required","status":503}` {
		t.Errorf("unstable error response: %s", response.Body.String())
	}
	return response.Code, slot.boundaries, db.reads
}

func TestConversationBoundary(t *testing.T) {
	for _, path := range []string{"clearUnread", "setUnread", "delete"} {
		for _, typ := range []uint8{1, 2} {
			for _, localSeq := range []uint64{0, 7} {
				t.Run(fmt.Sprintf("%s/type%d/local%d", path, typ, localSeq), func(t *testing.T) {
					status, writes, reads := conversationBoundaryRequest(t, path, typ, 0, localSeq, 42, 200)
					if status != 200 || len(writes) != 1 || writes[0] != 42 || reads != 0 {
						t.Errorf("must use channel leader boundary 42; got HTTP=%d boundaries=%v local_reads=%d", status, writes, reads)
					}
				})
			}
		}
	}
}

func TestConversationRemoteFailure(t *testing.T) {
	for _, path := range []string{"clearUnread", "setUnread", "delete"} {
		t.Run(path, func(t *testing.T) {
			status, writes, reads := conversationBoundaryRequest(t, path, 1, 0, 7, 42, 503)
			if status != 503 || len(writes) != 0 || reads != 0 {
				t.Errorf("remote failure must not commit a local fallback; HTTP=%d boundaries=%v local_reads=%d", status, writes, reads)
			}
		})
	}
}

func TestConversationInvalidArithmetic(t *testing.T) {
	t.Run("zero_sequence_with_unread", func(t *testing.T) {
		status, writes, _ := conversationBoundaryRequest(t, "setUnread", 2, 5, 0, 0, 200)
		if status != 200 || len(writes) != 0 {
			t.Errorf("zero sequence must not underflow; HTTP=%d boundaries=%v", status, writes)
		}
	})
	t.Run("negative_unread", func(t *testing.T) {
		status, writes, _ := conversationBoundaryRequest(t, "setUnread", 2, -1, 7, 42, 200)
		if status != 400 || len(writes) != 0 {
			t.Errorf("negative unread must be rejected; HTTP=%d boundaries=%v", status, writes)
		}
	})
}

func TestConversationBoundaryTimeout(t *testing.T) {
	for _, path := range []string{"clearUnread", "setUnread", "delete"} {
		t.Run(path, func(t *testing.T) {
			status, writes, reads := conversationBoundaryRequest(t, path, 1, 0, 7, 42, 504)
			if status != 503 || len(writes) != 0 || reads != 0 {
				t.Fatalf("status=%d writes=%v reads=%d", status, writes, reads)
			}
		})
	}
}
func TestConversationSetUnreadBoundary(t *testing.T) {
	status, writes, reads := conversationBoundaryRequest(t, "setUnread", 2, 3, 7, 42, 200)
	if status != 200 || len(writes) != 1 || writes[0] != 39 || reads != 0 {
		t.Fatalf("status=%d writes=%v reads=%d", status, writes, reads)
	}
}

func TestConversationBoundaryCanceled(t *testing.T) {
	for _, path := range []string{"clearUnread", "setUnread", "delete"} {
		t.Run(path, func(t *testing.T) {
			status, writes, reads := conversationBoundaryRequest(t, path, 1, 0, 7, 42, 499)
			if status != 503 || len(writes) != 0 || reads != 0 {
				t.Fatalf("status=%d writes=%v reads=%d", status, writes, reads)
			}
		})
	}
}
func TestConversationUnreadDoesNotRegress(t *testing.T) {
	for _, path := range []string{"clearUnread", "setUnread"} {
		t.Run(path, func(t *testing.T) {
			status, writes, reads := conversationBoundaryRequest(t, path, 2, 0, 7, 42, 200, 100)
			if status != 200 || len(writes) != 0 || reads != 0 {
				t.Fatalf("status=%d writes=%v reads=%d", status, writes, reads)
			}
		})
	}
}
