# SiltKV

A high-performance LSM-tree based key-value storage engine written in Go.

## Features

- **LSM-Tree Architecture**: Efficient write-optimized storage with automatic compaction
- **SkipList Memtable**: Fast in-memory data structure for recent writes (default 4MB)
- **Block-Based SSTable**: Data organized in 4KB blocks with sparse index for efficient reads
- **Sparse Index**: Binary search on block index to quickly locate data blocks
- **Bloom Filter**: Fast key existence checks to avoid unnecessary disk reads
- **Write-Ahead Log (WAL)**: Durability guarantee with automatic recovery
- **Automatic Compaction**: Background merging of SSTables to maintain read performance
- **Manifest Management**: Atomic updates for SSTable metadata

## Architecture

### Components

- **LSM**: LSM-tree implementation and management
  - Handles memtable rotation and flush coordination
  - Manages SSTable lifecycle and compaction
  - Maintains manifest for SSTable tracking

- **Memtable**: In-memory table for recent writes
  - SkipList-based implementation for O(log n) operations
  - Default size: 4MB (configurable)
  - Automatically flushed to SSTable when full
  - WAL-backed for durability

- **SSTable**: Sorted String Table for persistent storage
  - Block-based storage (4KB blocks)
  - Sparse index for efficient block lookup
  - Bloom filter for fast key existence checks
  - Footer with metadata (block index offset, bloom filter offset)

- **WAL**: Write-Ahead Log for durability
  - All writes logged before being applied to memtable
  - Automatic recovery on database open
  - Synced to disk when memtable is frozen (before flush)

### Read Path

1. Check active memtable (SkipList lookup)
2. Check immutable memtable (if exists)
3. Check SSTables in order (newest first):
   - Use Bloom filter for quick existence check
   - Use sparse index to find relevant block
   - Search within the block

### Write Path

1. Write to WAL (for durability)
2. Write to active memtable (SkipList)
3. When memtable is full:
   - Freeze active memtable → immutable
   - Create new active memtable
   - Flush immutable to SSTable in background

### Compaction

- Triggered when SSTable count reaches threshold (default: 4)
- Merges oldest SSTables into new ones
- Removes duplicate keys and tombstones
- Maintains sorted order

## Project Structure

```
SiltKV/
├── cmd/             # Demo programs and CLI tools
│   └── demo/        # Example programs (flush, compaction, recovery, etc.)
├── internal/        # Core implementation
│   ├── lsm/         # LSM-tree DB implementation
│   ├── memtable/    # SkipList-based memtable with WAL
│   ├── sstable/    # Block-based SSTable with sparse index
│   └── wal/         # Write-Ahead Log implementation
├── pkg/             # Public APIs
│   └── kv/          # High-level key-value API
├── benchmark/       # Performance benchmarks
└── README.md
```

## Getting Started

### Installation

```bash
go get github.com/return2faye/SiltKV
```

### Basic Usage

```go
package main

import (
    "fmt"
    "github.com/return2faye/SiltKV/pkg/kv"
)

func main() {
    // Open database
    db, err := kv.Open("/path/to/data")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Put a key-value pair
    err = db.Put("key1", "value1")
    if err != nil {
        panic(err)
    }

    // Get a value
    value, err := db.Get("key1")
    if err != nil {
        if err == kv.ErrNotFound {
            fmt.Println("Key not found")
        } else {
            panic(err)
        }
    } else {
        fmt.Printf("Value: %s\n", value)
    }

    // Delete a key
    err = db.Delete("key1")
    if err != nil {
        panic(err)
    }
}
```

### Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. -benchmem ./benchmark/...

# Run specific benchmark
go test -bench=BenchmarkPut -benchmem ./benchmark/...

# Run with detailed output
go test -bench=. -benchmem -benchtime=2s ./benchmark/...
```

See [benchmark/README.md](./benchmark/README.md) for more details.

### Demo Programs

```bash
# Run flush demo
go run cmd/demo/flush_demo.go

# Run compaction demo
go run cmd/demo/compaction_demo.go

# Run recovery demo
go run cmd/demo/recovery_demo.go
```

## Performance Characteristics

- **Write Performance**: Optimized for write-heavy workloads
  - Writes go to memtable first (in-memory)
  - Background flush to SSTable doesn't block writes
  - No per-operation disk sync (sync happens on memtable freeze)

- **Read Performance**: 
  - Fast reads from memtable (SkipList)
  - Sparse index enables direct block access
  - Bloom filter reduces unnecessary disk reads

- **Space Efficiency**:
  - Automatic compaction reduces space amplification
  - Tombstones mark deleted keys (cleaned during compaction)

## Configuration

Current default values:
- Memtable max size: 4MB
- SSTable block size: 4KB
- Compaction trigger: 4 SSTables
- Max SSTable file size: 64MB

## License

(To be determined)
