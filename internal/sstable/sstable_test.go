package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/return2faye/SiltKV/internal/memtable"
)

func TestFlushAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstPath := filepath.Join(tmpDir, "test.sst")

	// 2. Create memtable and write test data
	mt, err := memtable.NewMemtable(walPath)
	if err != nil {
		t.Fatalf("Failed to create memtable: %v", err)
	}
	defer mt.Close()

	testData := map[string]string{
		"key3": "value3",
		"key1": "value1",
		"key2": "value2",
		"key5": "value5",
		"key4": "value4",
	}

	for k, v := range testData {
		if err := mt.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	// 3. Freeze memtable before flushing
	mt.Freeze()

	// 4. Flush memtable into SSTable
	writer, err := NewWriter(sstPath)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	it := mt.NewIterator()
	if err := writer.WriteFromIterator(it); err != nil {
		writer.Close()
		t.Fatalf("Failed to flush: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// 5. Read back from SSTable and verify
	reader, err := NewReader(sstPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Verify that all written keys can be read back
	for k, expectedV := range testData {
		val, found, err := reader.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get error for %s: %v", k, err)
		}
		if !found {
			t.Errorf("Key %s not found", k)
			continue
		}
		if string(val) != expectedV {
			t.Errorf("Key %s: expected %s, got %s", k, expectedV, string(val))
		}
	}

	// Verify that a non-existent key is not found
	_, found, err := reader.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Get error for nonexistent key: %v", err)
	}
	if found {
		t.Error("Nonexistent key should not be found")
	}
}

func TestRecordsAcrossBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blocks.sst")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	value := make([]byte, 200)
	for i := 0; i < 100; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("key-%03d", i)), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.bloomFilter == nil {
		t.Fatal("bloom filter was not loaded")
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if _, found, err := r.Get(key); err != nil || !found {
			t.Fatalf("Get(%q): found=%v err=%v", key, found, err)
		}
	}
}

func TestBlockChecksumDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checksum.sst")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{'X'}, int64(8+8+len("key"))); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, err := r.Get([]byte("key")); !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf("Get error = %v, want %v", err, ErrCorruptSSTable)
	}
	it := r.NewIterator()
	if err := it.Next(); !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf("iterator error = %v, want %v", err, ErrCorruptSSTable)
	}
}

func TestTruncatedSSTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.sst")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	if _, err := NewReader(path); !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf("NewReader error = %v, want %v", err, ErrCorruptSSTable)
	}
}

func TestLegacySSTableReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sst")
	key, value := []byte("key"), []byte("value")
	block := make([]byte, 8, 8+len(key)+len(value))
	binary.LittleEndian.PutUint32(block[:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(block[4:], uint32(len(value)))
	block = append(block, key...)
	block = append(block, value...)
	index := &BlockIndex{}
	index.Add(key, 0)
	indexData := index.Serialize()
	bloom := NewBloomFilter(1, 0.01)
	bloom.Add(key)
	bloomData := bloom.Bytes()
	footer := (&Footer{
		BlockIndexOffset:  int64(len(block)),
		BlockIndexSize:    int64(len(indexData)),
		BloomFilterOffset: int64(len(block) + len(indexData)),
		MagicNumber:       legacyMagicNumber,
	}).Serialize()
	data := append(append(append(block, indexData...), bloomData...), footer...)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, found, err := r.Get(key)
	if err != nil || !found || string(got) != string(value) {
		t.Fatalf("Get legacy = %q, found=%v err=%v", got, found, err)
	}
}

func TestIteratorCorruption(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "corrupted.sst")

	// Create a file with invalid format (too short, no valid footer)
	f, err := os.Create(sstPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Write([]byte{0x01, 0x00, 0x00, 0x00})
	f.Close()

	_, err = NewReader(sstPath)
	if err == nil {
		t.Fatal("NewReader should fail on corrupt file")
	}
	if err != ErrCorruptSSTable {
		t.Errorf("Expected ErrCorruptSSTable, got: %v", err)
	}
}

func TestEmptySSTable(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "empty.sst")

	// Create a valid but empty SSTable via Writer (has footer/index/bloom)
	writer, err := NewWriter(sstPath)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	reader, err := NewReader(sstPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	it := reader.NewIterator()
	err = it.Next()
	if err != nil {
		t.Fatalf("Next on empty file should succeed, got: %v", err)
	}
	if it.Valid() {
		t.Error("Iterator should be invalid for empty file")
	}

	_, found, err := reader.Get([]byte("anykey"))
	if err != nil {
		t.Fatalf("Get on empty file should succeed, got: %v", err)
	}
	if found {
		t.Error("Get should return not found for empty file")
	}
}

func TestIteratorOrder(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstPath := filepath.Join(tmpDir, "test.sst")

	mt, err := memtable.NewMemtable(walPath)
	if err != nil {
		t.Fatalf("Failed to create memtable: %v", err)
	}
	defer mt.Close()

	// Put data in random order
	testKeys := []string{"key3", "key1", "key5", "key2", "key4"}
	for _, k := range testKeys {
		if err := mt.Put([]byte(k), []byte("value")); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	mt.Freeze()

	writer, err := NewWriter(sstPath)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	it := mt.NewIterator()
	if err := writer.WriteFromIterator(it); err != nil {
		writer.Close()
		t.Fatalf("Failed to flush: %v", err)
	}
	writer.Close()

	reader, err := NewReader(sstPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Verify iterator order
	sstIt := reader.NewIterator()
	expectedOrder := []string{"key1", "key2", "key3", "key4", "key5"}
	idx := 0

	if err := sstIt.Next(); err != nil {
		t.Fatalf("Failed to move to first record: %v", err)
	}

	for sstIt.Valid() {
		if idx >= len(expectedOrder) {
			t.Errorf("Iterator returned more items than expected")
			break
		}

		key := string(sstIt.Key())
		if key != expectedOrder[idx] {
			t.Errorf("Position %d: expected %s, got %s", idx, expectedOrder[idx], key)
		}

		if err := sstIt.Next(); err != nil {
			t.Fatalf("Failed to advance iterator: %v", err)
		}
		idx++
	}

	if idx != len(expectedOrder) {
		t.Errorf("Expected %d items, got %d", len(expectedOrder), idx)
	}
}
