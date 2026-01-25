package sstable

import (
	"bytes"
)

// MergeIterator merges multiple SSTable iterators into one sorted iterator.
// It handles duplicate keys by keeping the value from the newest SSTable.
type MergeIterator struct {
	iterators []*Iterator
	current   []*Iterator // iterators that have valid current key
	key       []byte
	value     []byte
}

// NewMergeIterator creates a new merge iterator from multiple SSTable readers.
// Readers should be ordered from newest to oldest.
func NewMergeIterator(readers []*Reader) (*MergeIterator, error) {
	iterators := make([]*Iterator, 0, len(readers))
	for _, r := range readers {
		if r != nil {
			it := r.NewIterator()
			if err := it.Next(); err != nil {
				// Skip corrupted iterators
				continue
			}
			if it.Valid() {
				iterators = append(iterators, it)
			}
		}
	}

	mi := &MergeIterator{
		iterators: iterators,
		current:   make([]*Iterator, 0, len(iterators)),
	}

	// Initialize by advancing all iterators
	if err := mi.advance(); err != nil {
		return nil, err
	}

	return mi, nil
}

// Valid returns true if the iterator has a valid current key.
func (mi *MergeIterator) Valid() bool {
	return len(mi.current) > 0
}

// Key returns the current key.
func (mi *MergeIterator) Key() []byte {
	return mi.key
}

// Value returns the current value.
func (mi *MergeIterator) Value() []byte {
	return mi.value
}

// Next advances the iterator to the next key.
func (mi *MergeIterator) Next() error {
	return mi.advance()
}

// advance finds the next key to return.
// It handles duplicates by keeping the value from the first (newest) iterator.
func (mi *MergeIterator) advance() error {
	mi.current = mi.current[:0]
	mi.key = nil
	mi.value = nil

	if len(mi.iterators) == 0 {
		return nil
	}

	// Find the smallest key among all iterators
	var minKey []byte
	for _, it := range mi.iterators {
		if !it.Valid() {
			continue
		}
		if minKey == nil || bytes.Compare(it.Key(), minKey) < 0 {
			minKey = it.Key()
		}
	}

	if minKey == nil {
		return nil
	}

	// Collect all iterators with the same key (newest first)
	for _, it := range mi.iterators {
		if !it.Valid() {
			continue
		}
		if bytes.Equal(it.Key(), minKey) {
			mi.current = append(mi.current, it)
		}
	}

	// Use the value from the first iterator (newest SSTable)
	if len(mi.current) > 0 {
		mi.key = mi.current[0].Key()
		mi.value = mi.current[0].Value()
	}

	// Advance all iterators with the same key
	for _, it := range mi.current {
		if err := it.Next(); err != nil {
			// Iterator exhausted, will be skipped in next advance
		}
	}

	return nil
}