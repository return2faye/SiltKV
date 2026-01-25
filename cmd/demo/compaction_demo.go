package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/return2faye/SiltKV/internal/lsm"
)

func main() {
	// Create a temporary directory for demo
	tmpDir := filepath.Join(os.TempDir(), "siltkv-compaction-demo")
	defer os.RemoveAll(tmpDir) // Clean up after demo

	fmt.Println("=== SiltKV Compaction Test ===")
	fmt.Printf("Data directory: %s\n\n", tmpDir)

	// Open DB
	fmt.Println("1. Opening DB...")
	db, err := lsm.Open(lsm.Options{DataDir: tmpDir})
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write enough data to trigger multiple flushes and compaction
	// Each flush creates ~4MB, so we need multiple flushes to trigger compaction
	fmt.Println("2. Writing data to trigger multiple flushes and compaction...")
	
	keyCounter := 0
	
	// Write data in batches to trigger multiple flushes
	for batch := 0; batch < 6; batch++ {
		fmt.Printf("  Batch %d: Writing keys...\n", batch+1)
		
		// Write ~800 keys per batch (each ~5KB) = ~4MB per batch
		for i := 0; i < 800; i++ {
			key := fmt.Sprintf("key-%05d", keyCounter)
			value := make([]byte, 5000) // 5KB per value
			for j := range value {
				value[j] = byte(keyCounter + j)
			}

			if err := db.Put([]byte(key), value); err != nil {
				log.Fatalf("Failed to put %s: %v", key, err)
			}
			keyCounter++

			// Check if flush happened (by checking for new WAL files)
			// This is a simple heuristic
		}
		
		// Give some time for flush to complete
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("  Total written: %d keys\n", keyCounter)

	// Wait for compaction to complete
	fmt.Println("\n3. Waiting for compaction to complete...")
	time.Sleep(2 * time.Second)

	// Check SSTable files
	fmt.Println("\n4. Checking SSTable files...")
	sstFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		log.Fatalf("Failed to list SSTable files: %v", err)
	}

	fmt.Printf("  Found %d SSTable file(s):\n", len(sstFiles))
	totalSize := int64(0)
	for _, f := range sstFiles {
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		size := info.Size()
		totalSize += size
		fmt.Printf("    %s (%d bytes, %.2f MB)\n", filepath.Base(f), size, float64(size)/(1024*1024))
	}
	fmt.Printf("  Total size: %.2f MB\n", float64(totalSize)/(1024*1024))

	// Verify data can be read back
	fmt.Println("\n5. Verifying data integrity...")
	verified := 0
	failed := 0

	// Test keys from different batches
	testKeys := []int{0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 4700, 4799}

	for _, keyNum := range testKeys {
		if keyNum >= keyCounter {
			continue
		}
		key := fmt.Sprintf("key-%05d", keyNum)
		expectedValue := make([]byte, 5000)
		for j := range expectedValue {
			expectedValue[j] = byte(keyNum + j)
		}

		val, found, err := db.Get([]byte(key))
		if err != nil {
			log.Printf("  ✗ Get error for %s: %v", key, err)
			failed++
			continue
		}
		if !found {
			log.Printf("  ✗ Key %s not found", key)
			failed++
			continue
		}

		// Verify value matches
		if len(val) != len(expectedValue) {
			log.Printf("  ✗ Key %s: value length mismatch, expected %d, got %d", key, len(expectedValue), len(val))
			failed++
			continue
		}

		match := true
		for j := range val {
			if val[j] != expectedValue[j] {
				match = false
				break
			}
		}

		if !match {
			log.Printf("  ✗ Key %s: value mismatch", key)
			failed++
			continue
		}

		verified++
		fmt.Printf("  ✓ %s\n", key)
	}

	fmt.Printf("\n6. Verification results: %d/%d passed", verified, len(testKeys))
	if failed > 0 {
		fmt.Printf(", %d failed", failed)
	}
	fmt.Println()

	// Check Manifest file
	fmt.Println("\n7. Checking Manifest file...")
	manifestPath := filepath.Join(tmpDir, "MANIFEST")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		log.Fatal("Manifest file not found!")
	}
	fmt.Printf("  ✓ Manifest file exists: %s\n", manifestPath)

	// Read and display Manifest content
	manifestContent, err := os.ReadFile(manifestPath)
	if err != nil {
		log.Printf("  Warning: Failed to read manifest: %v", err)
	} else {
		// Count lines (each line is a path)
		lines := 0
		for _, b := range manifestContent {
			if b == '\n' {
				lines++
			}
		}
		fmt.Printf("  Manifest contains %d SSTable path(s)\n", lines)
		fmt.Printf("  Manifest content:\n")
		fmt.Printf("    %s", string(manifestContent))
	}

	// Check if compaction happened (SSTable count should be controlled)
	fmt.Println("\n8. Compaction analysis:")
	if len(sstFiles) <= 4 {
		fmt.Printf("  ✓ SSTable count is controlled: %d files (compaction likely happened)\n", len(sstFiles))
	} else {
		fmt.Printf("  ⚠ SSTable count: %d files (compaction may not have triggered yet)\n", len(sstFiles))
	}

	// Check for compact files (indicates compaction happened)
	compactFiles := 0
	for _, f := range sstFiles {
		if filepath.Base(f)[:7] == "compact" {
			compactFiles++
		}
	}
	if compactFiles > 0 {
		fmt.Printf("  ✓ Found %d compacted SSTable file(s) (compaction happened!)\n", compactFiles)
	} else {
		fmt.Printf("  ⚠ No compacted files found (compaction may not have happened yet)\n")
	}

	fmt.Println("\n=== Compaction test completed! ===")
}
