# SiltKV

A high-performance LSM-tree based key-value storage engine.

## Project Structure

```
SiltKV/
├── cmd/             # Command-line tools (e.g., CLI for testing)
├── internal/        # Core logic: lsm, memtable, sstable, wal
│   ├── lsm/
│   ├── memtable/
│   ├── sstable/
│   └── wal/
├── pkg/             # Public APIs for external use
├── benchmark/       # Benchmark scripts
└── README.md
```

## Components

- **LSM**: LSM-tree implementation and management
- **Memtable**: In-memory table for recent writes
- **SSTable**: Sorted String Table for persistent storage
- **WAL**: Write-Ahead Log for durability

## Getting Started

(To be documented)

## License

(To be determined)
