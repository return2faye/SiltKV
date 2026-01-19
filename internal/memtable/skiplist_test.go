package memtable

import (
	"testing"
)

func TestSkipListPutGet(t *testing.T) {
	sl := NewSkipList()

	// Put some data
	testData := map[string]string{
		"key3": "value3",
		"key1": "value1",
		"key2": "value2",
		"key5": "value5",
		"key4": "value4",
	}

	for k, v := range testData {
		sl.Put([]byte(k), []byte(v))
	}

	// Get all data
	for k, expectedV := range testData {
		val, found := sl.Get([]byte(k))
		if !found {
			t.Errorf("Key %s not found", k)
			continue
		}
		if string(val) != expectedV {
			t.Errorf("Key %s: expected %s, got %s", k, expectedV, string(val))
		}
	}

	// Get non-existent key
	_, found := sl.Get([]byte("nonexistent"))
	if found {
		t.Error("Non-existent key should not be found")
	}
}

func TestSkipListUpdate(t *testing.T) {
	sl := NewSkipList()

	// Put initial value
	sl.Put([]byte("key1"), []byte("value1"))

	// Update it
	sl.Put([]byte("key1"), []byte("value1_updated"))

	// Verify update
	val, found := sl.Get([]byte("key1"))
	if !found {
		t.Fatal("Key should exist after update")
	}
	if string(val) != "value1_updated" {
		t.Errorf("Expected value1_updated, got %s", string(val))
	}
}

func TestSkipListDelete(t *testing.T) {
	sl := NewSkipList()

	// Put a key
	sl.Put([]byte("key1"), []byte("value1"))

	// Verify it exists
	val, found := sl.Get([]byte("key1"))
	if !found {
		t.Fatal("Key should exist before delete")
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", string(val))
	}

	// Delete it (tombstone: Put with nil value)
	sl.Put([]byte("key1"), nil)

	// Verify it's gone (Get returns false for tombstone)
	_, found = sl.Get([]byte("key1"))
	if found {
		t.Error("Key should not be found after delete (tombstone)")
	}
}

func TestSkipListIterator(t *testing.T) {
	sl := NewSkipList()

	// Put data in random order
	testData := []struct {
		key   string
		value string
	}{
		{"key3", "value3"},
		{"key1", "value1"},
		{"key2", "value2"},
		{"key5", "value5"},
		{"key4", "value4"},
	}

	for _, d := range testData {
		sl.Put([]byte(d.key), []byte(d.value))
	}

	// Iterate and verify order
	it := sl.NewIterator()
	expectedOrder := []string{"key1", "key2", "key3", "key4", "key5"}
	idx := 0

	for it.Valid() {
		if idx >= len(expectedOrder) {
			t.Errorf("Iterator returned more items than expected")
			break
		}

		key := string(it.Key())
		if key != expectedOrder[idx] {
			t.Errorf("Position %d: expected %s, got %s", idx, expectedOrder[idx], key)
		}

		it.Next()
		idx++
	}

	if idx != len(expectedOrder) {
		t.Errorf("Expected %d items, got %d", len(expectedOrder), idx)
	}
}

func TestSkipListSize(t *testing.T) {
	sl := NewSkipList()

	if sl.size != 0 {
		t.Errorf("New skip list should have size 0, got %d", sl.size)
	}

	// Put some data
	sl.Put([]byte("key1"), []byte("value1"))
	if sl.size != 1 {
		t.Errorf("Expected size 1, got %d", sl.size)
	}

	sl.Put([]byte("key2"), []byte("value2"))
	if sl.size != 2 {
		t.Errorf("Expected size 2, got %d", sl.size)
	}

	// Update existing key (should not increase size)
	sl.Put([]byte("key1"), []byte("value1_updated"))
	if sl.size != 2 {
		t.Errorf("Update should not increase size, expected 2, got %d", sl.size)
	}
}
