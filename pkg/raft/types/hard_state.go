package types

import (
	"encoding/binary"
	"fmt"
)

// HardState is local election state, independent of the last log's term.
// Its two fields must be persisted atomically and survive log truncation.
type HardState struct {
	Term uint32
	Vote uint64
}

func (s HardState) Marshal() []byte {
	data := make([]byte, 12)
	binary.BigEndian.PutUint32(data, s.Term)
	binary.BigEndian.PutUint64(data[4:], s.Vote)
	return data
}

func (s *HardState) Unmarshal(data []byte) error {
	if len(data) != 12 {
		return fmt.Errorf("invalid raft hard state length: %d", len(data))
	}
	s.Term = binary.BigEndian.Uint32(data)
	s.Vote = binary.BigEndian.Uint64(data[4:])
	if s.Term == 0 && s.Vote != 0 {
		return fmt.Errorf("raft vote without a term")
	}
	return nil
}
