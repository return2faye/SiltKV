package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/macz/SiltKV/internal/lsm"
)

func main() {
	// Use a persistent directory (not temp) to test recovery
	dataDir := "./test-db"
	defer os.RemoveAll(dataDir) // Clean up after demo

	fmt.Println("=== SiltKV Recovery Test ===")
	fmt.Printf("Data directory: %s\n\n", dataDir)

	// Step 1: Open DB and write data
	fmt.Println("1. Opening DB and writing data...")
	db1, err := lsm.Open(lsm.Options{DataDir: dataDir})
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}

	// Write some test data
	testData := map[string]string{
		"user:1001": "Alice",
		"user:1002": "Bob",
		"user:1003": "Charlie",
		"user:1004": "David",
		"user:1005": "Eve",
	}

	for k, v := range testData {
		if err := db1.Put([]byte(k), []byte(v)); err != nil {
			log.Fatalf("Failed to put %s: %v", k, err)
		}
		fmt.Printf("  Put: %s = %s\n", k, v)
	}

	// Write enough data to trigger flush
	fmt.Println("\n2. Writing more data to trigger flush...")
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%05d", i)
		value := make([]byte, 5000) // 5KB per value
		for j := range value {
			value[j] = byte(i + j)
		}
		if err := db1.Put([]byte(key), value); err != nil {
			log.Fatalf("Failed to put %s: %v", key, err)
		}
		if (i+1)%200 == 0 {
			fmt.Printf("  Written %d keys...\n", i+1)
		}
	}

	fmt.Println("\n3. Closing DB...")
	if err := db1.Close(); err != nil {
		log.Fatalf("Failed to close DB: %v", err)
	}

	// Step 2: Reopen DB and verify recovery
	fmt.Println("\n4. Reopening DB (testing recovery)...")
	db2, err := lsm.Open(lsm.Options{DataDir: dataDir})
	if err != nil {
		log.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db2.Close()

	// Verify original test data
	fmt.Println("\n5. Verifying original test data...")
	for k, expectedV := range testData {
		val, found, err := db2.Get([]byte(k))
		if err != nil {
			log.Fatalf("Failed to get %s: %v", k, err)
		}
		if !found {
			log.Fatalf("Key %s not found after recovery!", k)
		}
		if string(val) != expectedV {
			log.Fatalf("Key %s: expected %s, got %s", k, expectedV, string(val))
		}
		fmt.Printf("  ✓ %s = %s\n", k, string(val))
	}

	// Verify some flushed data
	fmt.Println("\n6. Verifying flushed data...")
	verifyKeys := []string{"key-00000", "key-00200", "key-00400", "key-00600", "key-00800"}
	verified := 0
	for _, keyStr := range verifyKeys {
		key := []byte(keyStr)
		val, found, err := db2.Get(key)
		if err != nil {
			log.Fatalf("Failed to get %s: %v", keyStr, err)
		}
		if !found {
			log.Fatalf("Key %s not found after recovery!", keyStr)
		}
		if len(val) != 5000 {
			log.Fatalf("Key %s: value length mismatch, expected 5000, got %d", keyStr, len(val))
		}
		verified++
		fmt.Printf("  ✓ %s (length: %d)\n", keyStr, len(val))
	}

	// Check Manifest file
	fmt.Println("\n7. Checking Manifest file...")
	manifestPath := filepath.Join(dataDir, "MANIFEST")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		log.Fatalf("Manifest file not found!")
	}
	fmt.Printf("  ✓ Manifest file exists: %s\n", manifestPath)

	// Check SSTable files
	fmt.Println("\n8. Checking SSTable files...")
	sstFiles, err := filepath.Glob(filepath.Join(dataDir, "*.sst"))
	if err != nil {
		log.Fatalf("Failed to list SSTable files: %v", err)
	}
	if len(sstFiles) == 0 {
		log.Fatal("No SSTable files found!")
	}
	fmt.Printf("  Found %d SSTable file(s):\n", len(sstFiles))
	for _, f := range sstFiles {
		info, _ := os.Stat(f)
		fmt.Printf("    %s (%d bytes)\n", filepath.Base(f), info.Size())
	}

	// Write new data after recovery
	fmt.Println("\n9. Writing new data after recovery...")
	newData := map[string]string{
		"user:2001": "Frank",
		"user:2002": "Grace",
	}
	for k, v := range newData {
		if err := db2.Put([]byte(k), []byte(v)); err != nil {
			log.Fatalf("Failed to put %s: %v", k, err)
		}
		fmt.Printf("  Put: %s = %s\n", k, v)
	}

	// Verify new data
	fmt.Println("\n10. Verifying new data...")
	for k, expectedV := range newData {
		val, found, err := db2.Get([]byte(k))
		if err != nil {
			log.Fatalf("Failed to get %s: %v", k, err)
		}
		if !found {
			log.Fatalf("Key %s not found!", k)
		}
		if string(val) != expectedV {
			log.Fatalf("Key %s: expected %s, got %s", k, expectedV, string(val))
		}
		fmt.Printf("  ✓ %s = %s\n", k, string(val))
	}

	fmt.Println("\n=== Recovery test completed successfully! ===")
}
