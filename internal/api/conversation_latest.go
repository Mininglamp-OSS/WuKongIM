package api

import (
	"context"
	"net/http"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
)

func (s *conversation) getChannelLastMsgSeq(ctx context.Context, channelID string, channelType uint8) (uint64, error) {
	timeout := options.G.Cluster.ReqTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return service.Cluster.GetChannelLastMessageSeq(ctx, channelID, channelType)
}

func respondConversationReadRetry(c *wkhttp.Context) {
	c.JSON(http.StatusServiceUnavailable, map[string]interface{}{"msg": "retry required", "status": http.StatusServiceUnavailable})
}
