# SiltKV Benchmarks

This directory contains benchmark tests for SiltKV to measure performance characteristics.

## Running Benchmarks

### Run all benchmarks:
```bash
go test -bench=. ./benchmark/...
```

### Run a specific benchmark:
```bash
go test -bench=BenchmarkPut ./benchmark/...
```

### Run with detailed output:
```bash
go test -bench=. -benchmem ./benchmark/...
```

### Run with CPU profiling:
```bash
go test -bench=. -cpuprofile=cpu.prof ./benchmark/...
go tool pprof cpu.prof
```

### Run with memory profiling:
```bash
go test -bench=. -memprofile=mem.prof ./benchmark/...
go tool pprof mem.prof
```

## Benchmark Descriptions

- **BenchmarkPut**: Measures write performance (memtable only)
- **BenchmarkGet**: Measures read performance from memtable
- **BenchmarkGetFromSSTable**: Measures read performance after data is flushed to SSTable
- **BenchmarkPutGet**: Measures mixed read-write performance
- **BenchmarkSequentialWrite**: Measures sequential write performance
- **BenchmarkRandomRead**: Measures random read performance
- **BenchmarkDelete**: Measures delete performance
- **BenchmarkWriteLargeValues**: Measures performance with large values (~4KB, web JSON payloads)
- **BenchmarkWriteSmallValues**: Measures performance with small values
- **BenchmarkConcurrentWrites**: Measures concurrent write performance
- **BenchmarkConcurrentReads**: Measures concurrent read performance

## Performance Results

Current benchmark results (Apple M4 Pro, Go 1.x):

| Benchmark | Performance | Memory | Allocations |
|-----------|-------------|--------|-------------|
| **BenchmarkPut** | 354.3 ns/op | 129 B/op | 4 allocs/op |
| **BenchmarkGet** | 93.17 ns/op | 38 B/op | 3 allocs/op |
| **BenchmarkGetFromSSTable** | 131.7 ns/op | 240 B/op | 3 allocs/op |
| **BenchmarkPutGet** | 532.0 ns/op | 176 B/op | 7 allocs/op |
| **BenchmarkSequentialWrite** | 420.0 ns/op | 179 B/op | 8 allocs/op |
| **BenchmarkRandomRead** | 212.1 ns/op | 48 B/op | 3 allocs/op |
| **BenchmarkDelete** | 347.7 ns/op | 88 B/op | 2 allocs/op |
| **BenchmarkWriteLargeValues** (~4KB) | 4897 ns/op | 6496 B/op | 11 allocs/op |
| **BenchmarkWriteSmallValues** | 437.8 ns/op | 160 B/op | 7 allocs/op |
| **BenchmarkConcurrentWrites** | 598.8 ns/op | 72 B/op | 5 allocs/op |
| **BenchmarkConcurrentReads** | 304.3 ns/op | 51 B/op | 4 allocs/op |

### Performance Highlights

- **Concurrent Write Performance**: ~6x improvement through fine-grained locking
- **Single-threaded Write**: ~3x improvement with optimized lock granularity
- **Read Performance**: Sub-200ns for memtable reads, ~130ns for SSTable reads
- **Concurrent Operations**: Excellent scalability with minimal lock contention

## Interpreting Results

Benchmark results show:
- **ns/op**: Nanoseconds per operation
- **B/op**: Bytes allocated per operation
- **allocs/op**: Number of allocations per operation

Lower is better for all metrics.

## Example Output

```
BenchmarkPut-8                   10000    123456 ns/op    1024 B/op    1 allocs/op
BenchmarkGet-8                   50000     12345 ns/op     512 B/op    1 allocs/op
```

This means:
- `BenchmarkPut` ran 10,000 iterations, averaging 123,456 nanoseconds per operation
- Each operation allocated 1024 bytes and made 1 allocation
- `-8` indicates it ran with 8 parallel goroutines (GOMAXPROCS)
