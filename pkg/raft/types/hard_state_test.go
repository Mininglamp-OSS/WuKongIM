package types

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHardStateRejectsCorruptRecord(t *testing.T) {
	for _, data := range [][]byte{nil, make([]byte, 11), make([]byte, 13), HardState{Vote: 2}.Marshal()} {
		var state HardState
		require.Error(t, state.Unmarshal(data))
	}
}
