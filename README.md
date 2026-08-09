# SiltKV

SiltKV 是一个用 Go 实现的单进程嵌入式 LSM-Tree KV 引擎，目标是用尽量少的组件完整演示写前日志、Memtable、SSTable、Manifest 与 Compaction 如何协同保证 point lookup 的正确性。

它目前适合作为存储引擎项目与实验基线，不应被描述为已经达到生产级或与 RocksDB 同级。

## 已实现范围

- `Put` / `Get` / `Delete`，newest-write-wins
- WAL + SkipList Memtable，默认 4 MiB 后轮转
- 后台 Flush，带 Block CRC32 校验的 4 KiB block-based SSTable
- sparse block index + Bloom Filter
- 计数触发的后台 Compaction、去重与 tombstone 回收
- Manifest 原子替换与多 WAL 段恢复
- 并发 API、后台周期性 `fsync`

边界：key 最大 128 B，value 为 1 B–4 KiB；空 value 被保留为 tombstone。暂不支持事务、快照、范围查询、压缩、block cache、TTL、跨进程并发写与在线配置。

## 架构与数据流

```text
Put/Delete
    │
    ▼
WAL append ──► SkipList Memtable
                  │ 4 MiB
                  ▼
             Immutable Memtable
                  │ background flush
                  ▼
       SSTable: [length | CRC32 | block]... | index | bloom | versioned footer
                  │ file count >= 4
                  ▼
              Compaction
```

### 写路径

1. 校验 key/value 大小；空 value 只用于删除标记。
2. 在 Memtable 写锁内先把 WAL record 写入 OS page cache，再更新 SkipList，保证并发写的运行态顺序与 WAL 回放顺序一致。
3. Memtable 达到 4 MiB 时冻结并 `fsync` WAL，切换到新 Memtable；旧表在后台写成 SSTable。
4. SSTable 完成 `fsync` 后先持久化 Manifest，再发布 reader，最后删除旧 WAL。失败时保留 WAL，避免不可恢复的数据丢失。

后台 Flush/Compaction 的首个错误会由后续 `Put` 和 `Close` 返回；系统不会在持久化链路已失败时继续静默接收写入。

`Put` 不执行逐条 `fsync`。因此：

| 故障 | 当前保证 |
|---|---|
| 进程异常退出 | 已返回的 WAL record 已交给 OS，可在重启时回放 |
| 主机掉电 / 内核崩溃 | 周期性 `fsync` 默认形成约 1 秒的潜在丢失窗口 |
| Freeze / 正常 Close | WAL 会在返回前 `fsync` |

如果业务要求每次 `Put` 都抗掉电，需要增加可配置的 sync-write 或 group commit；当前实现没有声称这一保证。

### 读路径

查询顺序固定为：

1. active Memtable
2. immutable Memtable
3. SSTables（newest first）

遇到 tombstone 会立即返回不存在，不能继续读取旧层，否则已删除数据会“复活”。SSTable 查询先用 Bloom Filter 排除确定不存在的文件，再通过 sparse index 二分定位候选 block，校验 Block CRC32，最后在不超过约 4 KiB 的 block 内顺序查找。

### SSTable

```text
[data blocks][sparse block index][Bloom Filter][32-byte footer]
```

- block index 保存每个 block 的 last key 与文件 offset
- Bloom Filter 使用双重哈希，默认目标误判率 1%
- footer 保存 index/bloom offset、index size 与 magic number
- reader 使用 `ReadAt`，不会共享文件游标
- 新文件写入带 CRC32 的 V2 block framing；reader 仍可读取未带 block checksum 的 V1 文件

### Compaction 与 Manifest

当 SSTable 数达到 4 时，后台合并最旧的 4 个文件：

- merge iterator 按 key 有序输出，重复 key 保留较新版本
- 因为输入包含全局最旧的一组文件，可以安全丢弃其中最终生效的 tombstone
- 输出文件以 64 MiB 为上限切分
- 先 `fsync` 新文件并原子更新 Manifest，再关闭和删除旧文件
- Manifest 在磁盘上按 oldest-first 保存，内存中按 newest-first 使用

这是简单的 count-based compaction，不是 leveled compaction。大数据集下若输出切分后无法降低文件数，会停止递归合并；只有观测到持续读放大后，才值得引入 level、size ratio 与 compaction score。

## 并发模型

- DB `RWMutex` 保护 Memtable/SSTable 元数据与 reader 生命周期
- Memtable 写锁串行化 WAL append 与 SkipList 更新
- SkipList 使用 `RWMutex` 支持并发读
- `atomic` 仅用于 Memtable size 与 frozen 状态
- WAL `fsync` 周期性在后台执行；写请求仍包含一次 WAL `write` syscall

因此更准确的说法是“并发安全，并将 Flush/Compaction/周期性 fsync 移出主要写路径”，而不是泛化为“细粒度锁带来高并发线性扩展”。

## 使用

```go
db, err := kv.Open("/path/to/data")
if err != nil {
    return err
}
defer db.Close()

if err := db.Put("key", "value"); err != nil {
    return err
}
value, err := db.Get("key")
if err := db.Delete("key"); err != nil {
    return err
}
_ = value
```

## 验证

```bash
# 正确性、崩溃恢复、跨 block、tombstone、compaction/reopen
go test ./...

# 并发竞态
go test -race ./...

# 静态检查
go vet ./...

# 可复现 benchmark；建议保留三次结果，不只挑最快值
go test -run='^$' -bench=. -benchmem -benchtime=2s -count=3 ./benchmark

# 对比不同 GOMAXPROCS
go test -run='^$' -bench='Concurrent' -benchmem -benchtime=2s -cpu=1,4,8 ./benchmark
```

benchmark 明确区分 Memtable hit、真实 SSTable hit、SSTable miss、持续 1 KiB 写、70/30 混合负载和 distinct-key 并发写。具体口径与最近一次本机结果见 [`benchmark/README.md`](benchmark/README.md)。

## 项目结构

```text
pkg/kv/          public API
internal/wal/    WAL encoding, checksum, recovery, fsync
internal/memtable/ SkipList Memtable
internal/sstable/ block, sparse index, Bloom Filter, iterator
internal/lsm/    read/write path, flush, manifest, compaction
benchmark/       reproducible Go benchmarks
cmd/demo/        standalone demos
```
