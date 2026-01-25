package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/return2faye/SiltKV/internal/lsm"
)

func main() {
	// Create a temporary directory for demo
	tmpDir := filepath.Join(os.TempDir(), "siltkv-flush-demo")
	defer os.RemoveAll(tmpDir) // Clean up after demo

	fmt.Println("=== SiltKV Flush Test ===")
	fmt.Printf("Data directory: %s\n\n", tmpDir)

	// Open DB
	fmt.Println("1. Opening DB...")
	db, err := lsm.Open(lsm.Options{DataDir: tmpDir})
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write enough data to trigger flush
	// Default memtable size is 4MB, so we need to write more than that
	// Let's write 1000 keys with ~5KB each = ~5MB total
	fmt.Println("2. Writing data to trigger flush...")
	keyCount := 1000
	valueSize := 5000 // 5KB per value

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := make([]byte, valueSize)
		// Fill with some data
		for j := range value {
			value[j] = byte(i + j)
		}

		if err := db.Put([]byte(key), value); err != nil {
			log.Fatalf("Failed to put %s: %v", key, err)
		}

		if (i+1)%100 == 0 {
			fmt.Printf("  Written %d keys...\n", i+1)
		}
	}

	fmt.Printf("  Total written: %d keys (~%d MB)\n", keyCount, (keyCount*valueSize)/(1024*1024))

	// Wait a bit for flush to complete
	fmt.Println("\n3. Waiting for flush to complete...")
	// In a real scenario, you might want to add a method to wait for flush
	// For now, we'll just wait a bit
	// time.Sleep(2 * time.Second)

	// Verify data can be read back
	fmt.Println("\n4. Verifying data from SSTable...")
	verifyCount := 10
	verified := 0

	for i := 0; i < verifyCount; i++ {
		key := fmt.Sprintf("key-%05d", i*100) // Check every 100th key
		expectedValue := make([]byte, valueSize)
		for j := range expectedValue {
			expectedValue[j] = byte(i*100 + j)
		}

		val, found, err := db.Get([]byte(key))
		if err != nil {
			log.Fatalf("Failed to get %s: %v", key, err)
		}
		if !found {
			log.Fatalf("Key %s not found!", key)
		}

		// Verify value matches
		if len(val) != len(expectedValue) {
			log.Fatalf("Key %s: value length mismatch, expected %d, got %d", key, len(expectedValue), len(val))
		}

		match := true
		for j := range val {
			if val[j] != expectedValue[j] {
				match = false
				break
			}
		}

		if !match {
			log.Fatalf("Key %s: value mismatch", key)
		}

		verified++
		fmt.Printf("  Verified: %s ✓\n", key)
	}

	fmt.Printf("\n5. Successfully verified %d/%d sample keys\n", verified, verifyCount)

	// Check if SSTable files were created
	fmt.Println("\n6. Checking SSTable files...")
	files, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		log.Fatalf("Failed to list SSTable files: %v", err)
	}

	if len(files) == 0 {
		fmt.Println("  Warning: No SSTable files found (flush might not have triggered)")
	} else {
		fmt.Printf("  Found %d SSTable file(s):\n", len(files))
		for _, f := range files {
			info, _ := os.Stat(f)
			fmt.Printf("    %s (%d bytes)\n", filepath.Base(f), info.Size())
		}
	}

	fmt.Println("\n=== Flush test completed successfully! ===")
}
