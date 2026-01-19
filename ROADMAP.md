# SiltKV Development Roadmap

This file tracks the remaining work for SiltKV and serves as a study log for the project.

## 1. LSM DB Controller (current focus)

- [ ] **Implement DB.Put / DB.Get / DB.Delete** in `internal/lsm/db.go`
  - [ ] `Put`: write to `active` memtable (no flush/rotation yet)
  - [ ] `Get`: read from `active` (and later `immutable` + SSTables)
  - [ ] `Delete`: implement as `Put(key, nil)` (tombstone)
- [ ] **Integrate flush with DB**
  - [ ] When `active.IsFull()`:
    - [ ] `Freeze` current `active` → move to `immutable`
    - [ ] Create new `active` with a new WAL file
    - [ ] Start a background goroutine to flush `immutable` to an SSTable (using `Writer.WriteFromIterator`)
  - [ ] After successful flush:
    - [ ] Open new `sstable.Reader` and append to `db.sstables` (newest first)
    - [ ] Safely close and remove old WAL for that memtable
    - [ ] Clear `immutable` pointer
- [ ] **DB.Close**
  - [ ] Close `active` / `immutable` memtables (and their WALs)
  - [ ] Close all `sstable.Reader`s

## 2. SSTable Improvements

- [ ] **Sparse index + block structure (V2)**
  - [ ] Define basic block format (e.g., fixed-size or key-count based)
  - [ ] While writing SSTable, record `firstKey + offset` per block
  - [ ] Write index region at the end of the SSTable file
  - [ ] Add in-memory sparse index structure in `sstable.Reader`
  - [ ] Update `Reader.Get` to:
    - [ ] Binary-search sparse index to locate a block
    - [ ] Linearly scan only that block
- [ ] **Optional Bloom filter per SSTable**
  - [ ] Build Bloom filter during flush
  - [ ] Store filter (simple on-disk format is enough for V1)
  - [ ] On `Get`, check Bloom filter before hitting disk

## 3. Manifest & Recovery

- [ ] **Manifest file**
  - [ ] Define simple manifest format (list of SSTable filenames and maybe levels)
  - [ ] On flush, append new SSTable entry to manifest
- [ ] **DB.Open recovery logic**
  - [ ] Read manifest to discover all existing SSTables
  - [ ] Open `sstable.Reader`s and populate `db.sstables`
  - [ ] Discover and replay WAL for latest active memtable
  - [ ] Decide how to handle incomplete flushes (e.g., temp files or missing SSTable)

## 4. Compaction (basic LSM behavior)

- [ ] **Basic two-file compaction**
  - [ ] Implement function to merge two (or more) SSTables into a new one
  - [ ] Honor tombstones (drop deleted keys)
  - [ ] Replace old SSTables with the new compacted file
- [ ] **Leveling strategy (simplified)**
  - [ ] Maintain levels (e.g., L0/L1/L2)
  - [ ] Define size/number thresholds for triggering compaction
  - [ ] Background compaction worker

## 5. Public API & CLI

- [ ] **Public Go API** (`pkg/`)
  - [ ] Wrap `lsm.DB` with a clean, user-friendly API (e.g., `Open`, `Put`, `Get`, `Delete`, `Close`)
  - [ ] Expose options: `DataDir`, memtable size, WAL sync strategy, etc.
- [ ] **CLI tool** (`cmd/`)
  - [ ] Basic commands: `put`, `get`, `delete`
  - [ ] Optional: a simple REPL or load/benchmark commands

## 6. Benchmarking & Profiling

- [ ] **Micro-benchmarks**
  - [ ] SkipList: `Put` / `Get`
  - [ ] WAL: `Write+Sync` vs `Write` only
  - [ ] SSTable: `Get` (linear), later: `Get` with sparse index
  - [ ] Memtable: `Put` / `Get`
- [ ] **End-to-end benchmarks**
  - [ ] DB: pure writes (sequential and random keys)
  - [ ] DB: read-heavy mixed workload (e.g., 80% `Get`, 20% `Put`)
- [ ] **Profiling**
  - [ ] Use `go test -bench` + `pprof` to identify:
    - [ ] Lock contention
    - [ ] Allocation hotspots
    - [ ] Disk I/O bottlenecks

## 7. Documentation & Cleanup

- [ ] **README improvements**
  - [ ] Add architecture overview (WAL → Memtable → SSTable → LSM)
  - [ ] Document write path, read path, flush, and compaction
  - [ ] Add section on safety features (checksums, bounds checks, tombstones)
- [ ] **Code cleanup**
  - [ ] Review error messages and exported API names
  - [ ] Ensure all comments are in clear English
  - [ ] Remove unused types (e.g., `Table` if not needed)

---

As of now, the following are implemented and tested:
- WAL with checksums, bounds checking, and fault-tolerant `Load`
- SkipList with iterator and deep-copy semantics
- Memtable (SkipList + WAL) with `Freeze` and `IsFull`
- SSTable V1 (writer/reader/iterator + safe `Get`)
- Unit tests for WAL, memtable, skiplist, sstable, and utils.
