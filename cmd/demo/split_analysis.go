package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/return2faye/SiltKV/internal/lsm"
	"github.com/return2faye/SiltKV/internal/sstable"
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "siltkv-split-analysis")
	defer os.RemoveAll(tmpDir)

	fmt.Println("=== SSTable Split Logic Analysis ===")
	fmt.Printf("Max SSTable file size: %d MB\n\n", sstable.MaxSSTableFileSize()/(1<<20))

	// Open DB
	db, err := lsm.Open(lsm.Options{DataDir: tmpDir})
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	maxSize := sstable.MaxSSTableFileSize()
	
	// Write data to create a scenario where we can test split
	// We'll write enough to create multiple SSTables, then compact them
	// The merged file should be large enough to trigger split
	
	fmt.Println("Writing data to trigger compaction...")
	
	// Write in batches to create multiple SSTables
	keyCounter := 0
	for batch := 0; batch < 8; batch++ {
		for i := 0; i < 800; i++ {
			key := fmt.Sprintf("key-%08d", keyCounter)
			value := make([]byte, 5000)
			for j := range value {
				value[j] = byte(keyCounter + j)
			}
			if err := db.Put([]byte(key), value); err != nil {
				log.Fatalf("Failed to put: %v", err)
			}
			keyCounter++
		}
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("Written %d keys, waiting for compaction...\n", keyCounter)
	time.Sleep(5 * time.Second)

	// Analyze files
	sstFiles, _ := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	
	fmt.Printf("\nFound %d SSTable files:\n", len(sstFiles))
	
	compactFiles := []struct {
		name string
		size int64
	}{}
	
	for _, f := range sstFiles {
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		name := filepath.Base(f)
		size := info.Size()
		
		if name[:7] == "compact" {
			compactFiles = append(compactFiles, struct {
				name string
				size int64
			}{name, size})
		}
		
		fmt.Printf("  %s: %.2f MB\n", name, float64(size)/(1024*1024))
	}

	fmt.Printf("\nSplit analysis:\n")
	if len(compactFiles) > 1 {
		fmt.Printf("  ✓ Split detected: %d compact files\n", len(compactFiles))
		for _, cf := range compactFiles {
			if cf.size > maxSize {
				fmt.Printf("    ⚠ %s: %.2f MB (EXCEEDS %d MB limit!)\n", 
					cf.name, float64(cf.size)/(1024*1024), maxSize/(1<<20))
			} else {
				fmt.Printf("    ✓ %s: %.2f MB (within limit)\n", 
					cf.name, float64(cf.size)/(1024*1024))
			}
		}
	} else if len(compactFiles) == 1 {
		cf := compactFiles[0]
		if cf.size > maxSize {
			fmt.Printf("  ⚠ PROBLEM: Only 1 compact file, but it's %.2f MB (exceeds %d MB limit!)\n",
				float64(cf.size)/(1024*1024), maxSize/(1<<20))
			fmt.Printf("    This means the split logic is NOT working correctly.\n")
			fmt.Printf("    Expected: File should have been split when it exceeded the limit.\n")
		} else {
			fmt.Printf("  ℹ Only 1 compact file: %.2f MB (within %d MB limit, split not needed)\n",
				float64(cf.size)/(1024*1024), maxSize/(1<<20))
		}
	}
}
