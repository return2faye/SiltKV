package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"github.com/return2faye/SiltKV/internal/utils"
)

// implementation of skiplist

const MaxLevel = 16

/*
basic structure
*/
type Node struct {
	key   []byte
	value []byte
	next  []*Node // denotes next node of IDXth level
}

type SkipList struct {
	head  *Node
	level int
	size  int
	mu    sync.RWMutex
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:  &Node{next: make([]*Node, MaxLevel)},
		level: 1,
	}
}

/*
random level
*/
func (sl *SkipList) randomlevel() int {
	level := 1
	for rand.Float64() < 0.5 && level < MaxLevel {
		level++
	}
	return level
}

func (sl *SkipList) Put(key, val []byte) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*Node, MaxLevel)
	curr := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		// key of next node smaller than key to insert, go on
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	// if already exist, update
	curr = curr.next[0]
	if curr != nil && bytes.Equal(curr.key, key) {
		curr.value = utils.CopyBytes(val)
		return
	}

	// generate random layer and insert
	lvl := sl.randomlevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.head
		}
		sl.level = lvl
	}

	newNode := &Node{
		key:   utils.CopyBytes(key),
		value: utils.CopyBytes(val),
		next:  make([]*Node, lvl),
	}

	for i := 0; i < lvl; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	// if tomebstone, not increase size
	if val != nil {
		sl.size++
	}
}

func (sl *SkipList) Get(key []byte) ([]byte, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
	}

	curr = curr.next[0]
	if curr != nil && bytes.Equal(curr.key, key) {
		if curr.value == nil {
			return nil, false
		}

		return curr.value, true
	}
	return nil, false
}



/*
Iterator
*/
type SLIterator struct {
	curr *Node
}

func (sl *SkipList) NewIterator() *SLIterator {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return &SLIterator{curr: sl.head.next[0]}
}

func (it *SLIterator) Valid() bool {
	return it.curr != nil
}

func (it *SLIterator) Next() {
	it.curr = it.curr.next[0]
}

func (it *SLIterator) Key() []byte {
	return it.curr.key
}

func (it *SLIterator) Value() []byte {
	return it.curr.value
}
