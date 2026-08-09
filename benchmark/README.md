# SiltKV Benchmarks

这些 benchmark 测量当前完整 API，而不是只测内部数据结构：写入包含 WAL `write` syscall，但不包含逐条 `fsync`；`fsync` 默认每秒执行，并在 Freeze/Close 时强制执行。

## 负载口径

| Benchmark | 测量内容 |
|---|---|
| `PutSmall` | 小 value、唯一 key 的 WAL + Memtable 写入 |
| `WriteSteadyState1KiB` | 1 KiB value，运行足够久时覆盖 Flush/Compaction |
| `GetMemtableHit` | active Memtable 命中 |
| `GetSSTableHit` | 强制 Flush、Close、Reopen 后的 SSTable 命中 |
| `GetSSTableMiss` | 真实 SSTable 非命中，覆盖 Bloom Filter 快速排除 |
| `Mixed70Read30Write` | 固定 70% hit read / 30% unique write |
| `ConcurrentWritesDistinctKeys` | 多 goroutine 写不同 key，避免覆盖造成虚高 |
| `ConcurrentReads` | 多 goroutine Memtable hit |

## 推荐命令

```bash
go test -run='^$' -bench=. -benchmem -benchtime=2s -count=3 ./benchmark
go test -run='^$' -bench='Concurrent' -benchmem -benchtime=2s -cpu=1,4,8 ./benchmark
```

结果只应在相同机器、Go 版本、`-benchtime`、durability 语义与数据规模下比较。报告中至少给出 `ns/op`、`B/op`、`allocs/op`、CPU、Go 版本与命令；不要把一次最快值写成稳定 SLA。

CPU / heap profiling：

```bash
go test -run='^$' -bench=WriteSteadyState -benchtime=10s -cpuprofile=cpu.out -memprofile=mem.out ./benchmark
go tool pprof cpu.out
```

## 最近验证

环境：2026-08-09，Apple M4 Pro，macOS 26.5.1，Go 1.25.5，darwin/arm64。

命令：

```bash
go test -run='^$' -bench='<benchmark name or group>' -benchmem -benchtime=1s -count=3 ./benchmark
```

三次运行中位数：

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| PutSmall | 1,419 | 141 | 4 |
| WriteSteadyState1KiB | 40,720（25.2 MB/s；31.2–46.1 μs） | 3,079 | 47 |
| GetMemtableHit | 98.3 | 24 | 3 |
| GetSSTableHit | 911.9 | 264 | 4 |
| GetSSTableMiss | 78.0 | 16 | 1 |
| Mixed70Read30Write | 1,038 | 76 | 3 |
| ConcurrentWritesDistinctKeys | 2,194 | 161 | 5 |
| ConcurrentReads | 170.7 | 24 | 3 |

并发 CPU 矩阵（单次 1 秒）：

| Benchmark | CPU=1 | CPU=4 | CPU=8 |
|---|---:|---:|---:|
| ConcurrentWritesDistinctKeys | 1,566 ns/op | 2,131 ns/op | 2,188 ns/op |
| ConcurrentReads | 102.7 ns/op | 90.3 ns/op | 128.8 ns/op |

结论：写路径受 WAL/Memtable 串行临界区限制，多核没有扩展；不能声称“并发写线性提升”。加入 Block CRC32 后，SSTable hit 通过复用读缓冲从 4,336 B/op 降至 264 B/op，同时中位延迟由 951.1 ns/op 降至 911.9 ns/op。持续写的 CRC/Flush 成本仍应通过长时间负载继续观察。

当前仓库验证结果：

```text
go test ./...          PASS
go test -race ./...    PASS
go vet ./...           PASS
```
