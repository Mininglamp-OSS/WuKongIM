package wkdb

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

func (wk *wukongDB) SaveRaftHardState(shardNo string, state types.HardState) error {
	return wk.shardDB(shardNo).Set(key.NewRaftHardStateKey(shardNo), state.Marshal(), pebble.Sync)
}

func (wk *wukongDB) RaftHardState(shardNo string) (types.HardState, error) {
	var state types.HardState
	data, closer, err := wk.shardDB(shardNo).Get(key.NewRaftHardStateKey(shardNo))
	if errors.Is(err, pebble.ErrNotFound) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	defer closer.Close()
	err = state.Unmarshal(data)
	return state, err
}
