package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/return2faye/SiltKV/internal/lsm"
)

func main() {
	// 1. Create a temporary directory for demo
	tmpDir := filepath.Join(os.TempDir(), "siltkv-demo")
	defer os.RemoveAll(tmpDir) // Clean up after demo

	fmt.Println("=== SiltKV Demo ===")
	fmt.Printf("Data directory: %s\n\n", tmpDir)

	// 2. Open DB
	fmt.Println("1. Opening DB...")
	db, err := lsm.Open(lsm.Options{DataDir: tmpDir})
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// 3. Put some data
	fmt.Println("2. Putting data...")
	testData := map[string]string{
		"user:1001": "Alice",
		"user:1002": "Bob",
		"user:1003": "Charlie",
		"user:1004": "David",
		"user:1005": "Eve",
	}

	for k, v := range testData {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			log.Fatalf("Failed to put %s: %v", k, err)
		}
		fmt.Printf("  Put: %s = %s\n", k, v)
	}

	// 4. Get all data
	fmt.Println("\n3. Getting data...")
	for k, expectedV := range testData {
		val, found, err := db.Get([]byte(k))
		if err != nil {
			log.Fatalf("Failed to get %s: %v", k, err)
		}
		if !found {
			log.Fatalf("Key %s not found!", k)
		}
		if string(val) != expectedV {
			log.Fatalf("Key %s: expected %s, got %s", k, expectedV, string(val))
		}
		fmt.Printf("  Get: %s = %s ✓\n", k, string(val))
	}

	// 5. Test non-existent key
	fmt.Println("\n4. Testing non-existent key...")
	_, found, err := db.Get([]byte("nonexistent"))
	if err != nil {
		log.Fatalf("Get error: %v", err)
	}
	if found {
		log.Fatal("Non-existent key should not be found!")
	}
	fmt.Println("  Get nonexistent key: not found ✓")

	// 6. Delete a key
	fmt.Println("\n5. Deleting a key...")
	keyToDelete := "user:1003"
	if err := db.Delete([]byte(keyToDelete)); err != nil {
		log.Fatalf("Failed to delete %s: %v", keyToDelete, err)
	}
	fmt.Printf("  Deleted: %s\n", keyToDelete)

	// 7. Verify deletion
	fmt.Println("\n6. Verifying deletion...")
	_, found, err = db.Get([]byte(keyToDelete))
	if err != nil {
		log.Fatalf("Get error: %v", err)
	}
	if found {
		log.Fatal("Deleted key should not be found!")
	}
	fmt.Printf("  Get %s: not found ✓ (tombstone)\n", keyToDelete)

	// 8. Verify other keys still exist
	fmt.Println("\n7. Verifying other keys still exist...")
	for k, expectedV := range testData {
		if k == keyToDelete {
			continue // Skip deleted key
		}
		val, found, err := db.Get([]byte(k))
		if err != nil {
			log.Fatalf("Failed to get %s: %v", k, err)
		}
		if !found {
			log.Fatalf("Key %s should still exist!", k)
		}
		if string(val) != expectedV {
			log.Fatalf("Key %s: expected %s, got %s", k, expectedV, string(val))
		}
		fmt.Printf("  Get: %s = %s ✓\n", k, string(val))
	}

	fmt.Println("\n=== Demo completed successfully! ===")
}