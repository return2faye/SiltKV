package benchmark

import (
	"fmt"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/return2faye/SiltKV/pkg/kv"
)

func setupDB(b *testing.B) (*kv.DB, string) {
	b.Helper()
	dir := filepath.Join(b.TempDir(), "bench-db")
	db, err := kv.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	return db, dir
}

func setupSSTableDB(b *testing.B) (*kv.DB, []string) {
	b.Helper()
	db, dir := setupDB(b)
	value := string(make([]byte, 100))
	for i := 0; i < 50_000; i++ { // >4 MiB: guarantees at least one flush
		if err := db.Put(fmt.Sprintf("key-%08d", i), value); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "*.sst")); len(files) == 0 {
		b.Fatal("setup did not create an SSTable")
	}
	db, err := kv.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	keys := make([]string, 1_000)
	for i := range keys { // early keys are in the flushed SSTable
		keys[i] = fmt.Sprintf("key-%08d", i)
	}
	return db, keys
}

func BenchmarkPutSmall(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(keys[i], "value"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteSteadyState1KiB(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	value := string(make([]byte, 1024))
	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(keys[i], value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetMemtableHit(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()
	keys := make([]string, 1_000)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
		if err := db.Put(keys[i], "value"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetSSTableHit(b *testing.B) {
	db, keys := setupSSTableDB(b)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetSSTableMiss(b *testing.B) {
	db, _ := setupSSTableDB(b)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get("missing-key"); err != kv.ErrNotFound {
			b.Fatalf("Get miss error = %v", err)
		}
	}
}

func BenchmarkMixed70Read30Write(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()
	readKeys := make([]string, 1_000)
	for i := range readKeys {
		readKeys[i] = strconv.Itoa(i)
		if err := db.Put(readKeys[i], "value"); err != nil {
			b.Fatal(err)
		}
	}
	writeKeys := make([]string, b.N)
	for i := range writeKeys {
		writeKeys[i] = "new-" + strconv.Itoa(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%10 < 3 {
			if err := db.Put(writeKeys[i], "value"); err != nil {
				b.Fatal(err)
			}
		} else if _, err := db.Get(readKeys[i%len(readKeys)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConcurrentWritesDistinctKeys(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()
	var sequence atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := strconv.FormatUint(sequence.Add(1), 10)
			if err := db.Put(key, "value"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkConcurrentReads(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()
	keys := make([]string, 1_000)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
		if err := db.Put(keys[i], "value"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := db.Get(keys[i%len(keys)]); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}
