package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/macz/SiltKV/internal/lsm"
	"github.com/macz/SiltKV/internal/sstable"
)

func main() {
	// Create a temporary directory for demo
	tmpDir := filepath.Join(os.TempDir(), "siltkv-split-test")
	defer os.RemoveAll(tmpDir)

	fmt.Println("=== SiltKV File Split Test ===")
	fmt.Printf("Data directory: %s\n", tmpDir)
	fmt.Printf("Max SSTable file size: %d MB\n\n", sstable.MaxSSTableFileSize()/(1<<20))

	// Open DB
	fmt.Println("1. Opening DB...")
	db, err := lsm.Open(lsm.Options{DataDir: tmpDir})
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write enough data to trigger compaction with large merged file
	// We need to create multiple SSTables first, then compact them
	// The merged file should exceed the limit and trigger split
	
	maxSize := sstable.MaxSSTableFileSize()
	fmt.Printf("2. Writing data to create large merged file (target: > %d MB)...\n", maxSize/(1<<20))
	
	// Calculate how many keys we need
	// Each key-value: key (10 bytes) + value (5000 bytes) + header (8 bytes) = ~5018 bytes
	// To exceed maxSize, we need: maxSize / 5018 keys
	keysNeeded := int((maxSize * 2) / 5018) // Write 2x the limit to ensure split
	
	fmt.Printf("   Need ~%d keys to create %d MB of data\n", keysNeeded, (maxSize*2)/(1<<20))
	
	keyCounter := 0
	batchSize := 800 // ~4MB per batch to trigger flush
	
	for batch := 0; keyCounter < keysNeeded; batch++ {
		fmt.Printf("   Batch %d: Writing keys %d-%d...\n", batch+1, keyCounter, keyCounter+batchSize-1)
		
		for i := 0; i < batchSize && keyCounter < keysNeeded; i++ {
			key := fmt.Sprintf("key-%08d", keyCounter)
			value := make([]byte, 5000)
			for j := range value {
				value[j] = byte(keyCounter + j)
			}

			if err := db.Put([]byte(key), value); err != nil {
				log.Fatalf("Failed to put %s: %v", key, err)
			}
			keyCounter++
		}
		
		// Wait for flush
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("   Total written: %d keys\n", keyCounter)

	// Wait for compaction and split to complete
	fmt.Println("\n3. Waiting for compaction and split to complete...")
	time.Sleep(5 * time.Second)

	// Check SSTable files
	fmt.Println("\n4. Checking SSTable files for split...")
	sstFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		log.Fatalf("Failed to list SSTable files: %v", err)
	}

	fmt.Printf("  Found %d SSTable file(s):\n", len(sstFiles))
	
	compactFiles := make(map[string]int64) // filename -> size
	activeFiles := make(map[string]int64)
	
	for _, f := range sstFiles {
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		size := info.Size()
		name := filepath.Base(f)
		
		if name[:7] == "compact" {
			compactFiles[name] = size
		} else {
			activeFiles[name] = size
		}
		
		fmt.Printf("    %s (%d bytes, %.2f MB)\n", name, size, float64(size)/(1024*1024))
	}

	// Analyze split
	fmt.Println("\n5. Split analysis:")
	if len(compactFiles) > 1 {
		fmt.Printf("  ✓ File split detected! Found %d compact files\n", len(compactFiles))
		for name, size := range compactFiles {
			if size > maxSize {
				fmt.Printf("    ⚠ %s is %.2f MB (exceeds %d MB limit!)\n", 
					name, float64(size)/(1024*1024), maxSize/(1<<20))
			} else {
				fmt.Printf("    ✓ %s is %.2f MB (within limit)\n", 
					name, float64(size)/(1024*1024))
			}
		}
	} else if len(compactFiles) == 1 {
		for _, size := range compactFiles {
			if size > maxSize {
				fmt.Printf("  ⚠ Only 1 compact file, but it's %.2f MB (exceeds %d MB limit!)\n",
					float64(size)/(1024*1024), maxSize/(1<<20))
				fmt.Printf("    This suggests the split logic may not be working correctly.\n")
			} else {
				fmt.Printf("  ℹ Only 1 compact file: %.2f MB (within %d MB limit, split not needed)\n",
					float64(size)/(1024*1024), maxSize/(1<<20))
			}
		}
	} else {
		fmt.Printf("  ⚠ No compact files found (compaction may not have happened)\n")
	}

	// Verify data integrity
	fmt.Println("\n6. Verifying data integrity...")
	testKeys := []int{0, 1000, 5000, 10000, 20000, 30000, keyCounter - 1}
	verified := 0
	
	for _, keyNum := range testKeys {
		if keyNum >= keyCounter {
			continue
		}
		key := fmt.Sprintf("key-%08d", keyNum)
		expectedValue := make([]byte, 5000)
		for j := range expectedValue {
			expectedValue[j] = byte(keyNum + j)
		}

		val, found, err := db.Get([]byte(key))
		if err != nil || !found {
			fmt.Printf("  ✗ %s: not found or error\n", key)
			continue
		}

		if len(val) != len(expectedValue) {
			fmt.Printf("  ✗ %s: length mismatch\n", key)
			continue
		}

		match := true
		for j := range val {
			if val[j] != expectedValue[j] {
				match = false
				break
			}
		}

		if match {
			verified++
			fmt.Printf("  ✓ %s\n", key)
		} else {
			fmt.Printf("  ✗ %s: value mismatch\n", key)
		}
	}

	fmt.Printf("\n7. Verification: %d/%d passed\n", verified, len(testKeys))
	fmt.Println("\n=== Split test completed! ===")
}
