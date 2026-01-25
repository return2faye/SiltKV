package benchmark

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/return2faye/SiltKV/pkg/kv"
)

// setupDB creates a temporary database for benchmarking
func setupDB(b *testing.B) (*kv.DB, string) {
	tmpDir := filepath.Join(b.TempDir(), "bench-db")
	db, err := kv.Open(tmpDir)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	return db, tmpDir
}

// BenchmarkPut measures the performance of Put operations
func BenchmarkPut(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	// Pre-generate keys and values to avoid allocation in benchmark
	keys := make([]string, b.N)
	values := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		values[i] = fmt.Sprintf("value-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := db.Put(keys[i], values[i]); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

// BenchmarkGet measures the performance of Get operations from memtable
func BenchmarkGet(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	// Pre-populate with data
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	// Pre-generate keys to read
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key-%d", i%numKeys)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(keys[i])
		if err != nil && err != kv.ErrNotFound {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkGetFromSSTable measures Get performance after data is flushed to SSTable
func BenchmarkGetFromSSTable(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	// Write enough data to trigger flush to SSTable
	// Assuming memtable max size is around 1MB, write ~10MB to ensure flush
	numKeys := 10000
	valueSize := 100

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(i + j)
		}
		if err := db.Put(key, string(value)); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	// Wait a bit for flush to complete (in real scenario, you'd wait for flush)
	// For benchmark, we'll just read from whatever is available

	// Pre-generate keys to read
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key-%08d", i%numKeys)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(keys[i])
		if err != nil && err != kv.ErrNotFound {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkPutGet measures mixed Put and Get operations
func BenchmarkPutGet(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	// Pre-generate keys and values
	keys := make([]string, b.N)
	values := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		values[i] = fmt.Sprintf("value-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Put
		if err := db.Put(keys[i], values[i]); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
		// Get
		_, err := db.Get(keys[i])
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkSequentialWrite measures sequential write performance
func BenchmarkSequentialWrite(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%010d", i)
		value := fmt.Sprintf("value-%010d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

// BenchmarkRandomRead measures random read performance
func BenchmarkRandomRead(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	// Pre-populate with data
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		value := fmt.Sprintf("value-%08d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	// Generate random keys
	rng := rand.New(rand.NewSource(42))
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key-%08d", rng.Intn(numKeys))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(keys[i])
		if err != nil && err != kv.ErrNotFound {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkDelete measures delete performance
func BenchmarkDelete(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	// Pre-populate with data
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		if err := db.Put(keys[i], fmt.Sprintf("value-%d", i)); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := db.Delete(keys[i]); err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
}

// BenchmarkWriteLargeValues measures performance with large values
func BenchmarkWriteLargeValues(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	// Generate large value (10KB)
	largeValue := make([]byte, 10*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	valueStr := string(largeValue)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := db.Put(key, valueStr); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

// BenchmarkWriteSmallValues measures performance with small values
func BenchmarkWriteSmallValues(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("v%d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

// BenchmarkConcurrentWrites measures concurrent write performance
func BenchmarkConcurrentWrites(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := db.Put(key, value); err != nil {
				b.Fatalf("Put failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkConcurrentReads measures concurrent read performance
func BenchmarkConcurrentReads(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	// Pre-populate with data
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		for pb.Next() {
			key := fmt.Sprintf("key-%d", rng.Intn(numKeys))
			_, err := db.Get(key)
			if err != nil && err != kv.ErrNotFound {
				b.Fatalf("Get failed: %v", err)
			}
		}
	})
}
