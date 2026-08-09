package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync"

	"github.com/return2faye/SiltKV/internal/memtable"
	"github.com/return2faye/SiltKV/internal/utils"
)

const (
	maxSSTableKeySize   = 128      // 128B - maximum key size for SSTable
	maxSSTableValueSize = 4 * 1024 // 4KB - maximum value size for SSTable
	maxSSTableFileSize  = 64 << 20 // 64MB - maximum size for a single SSTable file
	maxBlockBufferSize  = 16 + maxSSTableKeySize + maxSSTableValueSize
)

var blockBufferPool = sync.Pool{New: func() any {
	return make([]byte, maxBlockBufferSize)
}}

var (
	// ErrCorruptSSTable is returned when an SSTable file has an invalid layout
	// (e.g. missing or malformed footer, invalid offsets, etc.).
	ErrCorruptSSTable = errors.New("sstable: corrupt file")
)

// MaxSSTableFileSize returns the maximum size for a single SSTable file.
func MaxSSTableFileSize() int64 {
	return maxSSTableFileSize
}

// abstraction of SSTable
// read single .sst file
type Table struct {
	file *os.File
	path string
}

// flush memtable into SSTable file
type Writer struct {
	file            *os.File
	fileSize        int64
	blockIndex      *BlockIndex  // Block index for sparse indexing
	bloomFilter     *BloomFilter // Bloom filter for fast key existence check
	bloomHashes     []bloomHash
	currentBlock    []byte // Current block buffer being written
	blockOffset     int64  // Starting offset of the current block
	firstKeyInBlock []byte // First key in the current block (for block start)
	lastKeyInBlock  []byte // Last key in the current block (for sparse index)
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &Writer{
		file:            f,
		fileSize:        0,
		blockIndex:      &BlockIndex{Entries: make([]BlockIndexEntry, 0)},
		bloomFilter:     nil, // Will be initialized later
		currentBlock:    make([]byte, 0, BlockSize),
		blockOffset:     0,
		firstKeyInBlock: nil,
		lastKeyInBlock:  nil,
	}, nil
}

// flushCurrentBlock writes the current block to the file and adds it to the Block Index
func (w *Writer) flushCurrentBlock() error {
	if len(w.currentBlock) == 0 {
		return nil
	}

	// Record the starting offset of the block
	blockOffset := w.fileSize

	// Frame every block so readers can validate it before parsing records.
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[:4], uint32(len(w.currentBlock)))
	binary.LittleEndian.PutUint32(header[4:], crc32.ChecksumIEEE(w.currentBlock))
	if _, err := w.file.Write(header); err != nil {
		return err
	}
	if _, err := w.file.Write(w.currentBlock); err != nil {
		return err
	}

	// Add this block's last key to the sparse index (last key is better for lookup)
	if w.lastKeyInBlock != nil {
		w.blockIndex.Add(w.lastKeyInBlock, blockOffset)
	}

	// Update file size
	w.fileSize += int64(len(header) + len(w.currentBlock))

	// Reset current block (preserve capacity)
	w.currentBlock = w.currentBlock[:0]
	w.firstKeyInBlock = nil
	w.lastKeyInBlock = nil
	w.blockOffset = w.fileSize

	return nil
}

// writeRecordToBlock writes a record to the current block
// Returns true if the block is full and needs to be flushed
func (w *Writer) writeRecordToBlock(key, value []byte) (bool, error) {
	klen := uint32(len(key))
	vlen := uint32(len(value))
	recordSize := 8 + len(key) + len(value)

	// Check if the record can fit in the current block
	flushed := false
	if len(w.currentBlock)+recordSize > BlockSize && len(w.currentBlock) > 0 {
		if err := w.flushCurrentBlock(); err != nil {
			return false, err
		}
		flushed = true
	}

	if w.firstKeyInBlock == nil {
		w.firstKeyInBlock = utils.CopyBytes(key)
	}
	w.lastKeyInBlock = utils.CopyBytes(key)

	// Write the record to the block buffer
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], klen)
	binary.LittleEndian.PutUint32(header[4:8], vlen)

	w.currentBlock = append(w.currentBlock, header...)
	w.currentBlock = append(w.currentBlock, key...)
	w.currentBlock = append(w.currentBlock, value...)

	return flushed, nil
}

func (w *Writer) Close() (retErr error) {
	if w.file == nil {
		return nil
	}
	defer func() {
		closeErr := w.file.Close()
		w.file = nil
		if retErr == nil {
			retErr = closeErr
		}
	}()

	// 1. Flush remaining block
	if err := w.flushCurrentBlock(); err != nil {
		return err
	}

	// 2. Write Block Index
	blockIndexData := w.blockIndex.Serialize()
	blockIndexOffset := w.fileSize
	if _, err := w.file.Write(blockIndexData); err != nil {
		return err
	}
	blockIndexSize := int64(len(blockIndexData))
	w.fileSize += blockIndexSize

	// 3. Build the Bloom Filter from the actual key count, then write it.
	if w.bloomFilter == nil {
		w.bloomFilter = NewBloomFilter(uint32(len(w.bloomHashes)), 0.01)
		for _, hash := range w.bloomHashes {
			w.bloomFilter.addHash(hash)
		}
	}
	bloomFilterData := w.bloomFilter.Bytes()
	bloomFilterOffset := w.fileSize
	if _, err := w.file.Write(bloomFilterData); err != nil {
		return err
	}
	w.fileSize += int64(len(bloomFilterData))

	// 4. Write Footer
	footer := &Footer{
		BloomFilterOffset: bloomFilterOffset,
		BlockIndexOffset:  blockIndexOffset,
		BlockIndexSize:    blockIndexSize,
		MagicNumber:       MagicNumber,
	}
	footerData := footer.Serialize()
	if _, err := w.file.Write(footerData); err != nil {
		return err
	}
	w.fileSize += int64(len(footerData))

	if err := w.file.Sync(); err != nil {
		return err
	}
	return nil
}

// WriteFromIterator writes all key-value pairs from the iterator to the SSTable
// Data will be organized into multiple blocks, and a Bloom Filter and sparse index will be built
func (w *Writer) WriteFromIterator(it *memtable.SLIterator) error {
	if w.file == nil {
		return os.ErrInvalid
	}

	// Iterate through the iterator and write data
	for it.Valid() {
		key := it.Key()
		val := it.Value()

		// Add to Bloom Filter
		w.bloomHashes = append(w.bloomHashes, hashBloomKey(key))

		// Write to block
		_, err := w.writeRecordToBlock(key, val)
		if err != nil {
			return err
		}

		it.Next()
	}

	return nil
}

// Write writes a single key-value pair to the SSTable.
// Returns the current file size after write.
func (w *Writer) Write(key, value []byte) (int64, error) {
	if w.file == nil {
		return 0, os.ErrInvalid
	}

	// Add to Bloom Filter
	w.bloomHashes = append(w.bloomHashes, hashBloomKey(key))

	// Write to block
	_, err := w.writeRecordToBlock(key, value)
	if err != nil {
		return 0, err
	}

	return w.fileSize, nil
}

// Size returns the current file size.
func (w *Writer) Size() int64 {
	return w.fileSize
}

// Read from SSTable files
type Reader struct {
	file        *os.File
	fileSize    int64
	path        string
	footer      *Footer
	blockIndex  *BlockIndex
	bloomFilter *BloomFilter
	initialized bool
}

func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	reader := &Reader{
		file:        f,
		fileSize:    stat.Size(),
		path:        path,
		initialized: false,
	}

	// Initialize metadata (footer, block index, bloom filter)
	if err := reader.initialize(); err != nil {
		f.Close()
		return nil, err
	}

	return reader, nil
}

// initialize loads footer, block index, and bloom filter from the file
func (r *Reader) initialize() error {
	if r.initialized {
		return nil
	}

	// All SSTables are required to use the new format with footer/index/bloom.
	// A valid file must be at least 32 bytes to hold the footer.
	if r.fileSize < 32 {
		return ErrCorruptSSTable
	}

	// Read footer (last 32 bytes).
	footerData := make([]byte, 32)
	if _, err := r.file.ReadAt(footerData, r.fileSize-32); err != nil {
		return ErrCorruptSSTable
	}

	footer, err := DeserializeFooter(footerData)
	if err != nil {
		return ErrCorruptSSTable
	}
	r.footer = footer

	// Validate footer offsets
	footerOffset := r.fileSize - 32
	if footer.BlockIndexOffset < 0 || footer.BlockIndexSize < 0 ||
		footer.BloomFilterOffset < 0 ||
		footer.BlockIndexOffset+footer.BlockIndexSize != footer.BloomFilterOffset ||
		footer.BloomFilterOffset > footerOffset {
		return ErrCorruptSSTable
	}

	// Read block index
	if footer.BlockIndexSize > 0 && footer.BlockIndexOffset+footer.BlockIndexSize <= r.fileSize {
		blockIndexData := make([]byte, footer.BlockIndexSize)
		if _, err := r.file.ReadAt(blockIndexData, footer.BlockIndexOffset); err != nil {
			return ErrCorruptSSTable
		}

		blockIndex, err := DeserializeBlockIndex(blockIndexData)
		if err != nil {
			return ErrCorruptSSTable
		}
		r.blockIndex = blockIndex
	}

	// Read bloom filter
	if footer.BloomFilterOffset < footerOffset {
		bloomFilterSize := footerOffset - footer.BloomFilterOffset
		if bloomFilterSize > 0 && bloomFilterSize < 16*1024*1024 {
			bloomFilterData := make([]byte, bloomFilterSize)
			if _, err := r.file.ReadAt(bloomFilterData, footer.BloomFilterOffset); err != nil {
				return ErrCorruptSSTable
			}

			bloomFilter, err := LoadBloomFilter(bloomFilterData)
			if err != nil {
				return ErrCorruptSSTable
			}
			r.bloomFilter = bloomFilter
		}
	}

	r.initialized = true
	return nil
}

// Path returns the file path of this SSTable.
func (r *Reader) Path() string {
	return r.path
}

func (r *Reader) Close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

func (r *Reader) Get(key []byte) ([]byte, bool, error) {
	if r == nil || r.file == nil {
		return nil, false, os.ErrInvalid
	}

	// Initialize (if not already initialized)
	if !r.initialized {
		if err := r.initialize(); err != nil {
			return nil, false, err
		}
	}

	// New format: use Bloom Filter and Block Index
	// 1. Quick check with Bloom Filter
	if r.bloomFilter != nil && !r.bloomFilter.MayContain(key) {
		// Key definitely not in this SSTable
		return nil, false, nil
	}

	// 2. Find the block that might contain the key
	blockOffset := r.blockIndex.FindBlock(key)
	if blockOffset < 0 {
		return nil, false, nil
	}

	// 3. Search within the block
	return r.searchInBlock(key, blockOffset)
}

func (r *Reader) readBlock(blockOffset int64, buf []byte) ([]byte, error) {
	blockEnd := r.footer.BlockIndexOffset
	for _, entry := range r.blockIndex.Entries {
		if entry.Offset > blockOffset {
			blockEnd = entry.Offset
			break
		}
	}

	blockSize := blockEnd - blockOffset
	if blockSize <= 0 || blockSize > int64(len(buf)) {
		return nil, ErrCorruptSSTable
	}

	blockData := buf[:int(blockSize)]
	if _, err := r.file.ReadAt(blockData, blockOffset); err != nil {
		return nil, ErrCorruptSSTable
	}
	if r.footer.MagicNumber == legacyMagicNumber {
		return blockData, nil
	}
	if len(blockData) < 8 {
		return nil, ErrCorruptSSTable
	}
	payload := blockData[8:]
	if binary.LittleEndian.Uint32(blockData[:4]) != uint32(len(payload)) ||
		binary.LittleEndian.Uint32(blockData[4:8]) != crc32.ChecksumIEEE(payload) {
		return nil, ErrCorruptSSTable
	}
	return payload, nil
}

// searchInBlock searches for a key within the specified block
func (r *Reader) searchInBlock(key []byte, blockOffset int64) ([]byte, bool, error) {
	buf := blockBufferPool.Get().([]byte)
	defer blockBufferPool.Put(buf)
	blockData, err := r.readBlock(blockOffset, buf)
	if err != nil {
		return nil, false, err
	}
	blockSize := int64(len(blockData))

	// Parse the block and search for the key
	pos := int64(0)
	for pos < blockSize {
		if pos+8 > blockSize {
			return nil, false, ErrCorruptSSTable
		}

		// Read header
		klen := binary.LittleEndian.Uint32(blockData[pos : pos+4])
		vlen := binary.LittleEndian.Uint32(blockData[pos+4 : pos+8])

		if klen > maxSSTableKeySize || vlen > maxSSTableValueSize {
			return nil, false, ErrCorruptSSTable
		}

		totalLen := int64(klen) + int64(vlen)
		if pos+8+totalLen > blockSize {
			return nil, false, ErrCorruptSSTable
		}

		recordKey := blockData[pos+8 : pos+8+int64(klen)]
		cmp := bytes.Compare(recordKey, key)

		if cmp == 0 {
			if vlen == 0 {
				return nil, true, nil
			}
			recordValue := blockData[pos+8+int64(klen) : pos+8+totalLen]
			return utils.CopyBytes(recordValue), true, nil
		}

		if cmp > 0 {
			// Key is not in this block (keys are sorted)
			return nil, false, nil
		}

		pos += 8 + totalLen
	}

	return nil, false, nil
}

type Iterator struct {
	r          *Reader
	blockIndex int
	blockBuf   [maxBlockBufferSize]byte
	blockData  []byte
	pos        int
	key        []byte
	val        []byte
	eof        bool
}

func (r *Reader) NewIterator() *Iterator {
	// Initialize (if not already initialized)
	if !r.initialized {
		r.initialize()
	}

	return &Iterator{r: r}
}

func (it *Iterator) Valid() bool {
	return !it.eof && it.key != nil
}

func (it *Iterator) Key() []byte {
	return it.key
}

func (it *Iterator) Value() []byte {
	return it.val
}

func (it *Iterator) Next() error {
	if it.eof {
		return nil
	}
	if it.r == nil || it.r.file == nil {
		return os.ErrInvalid
	}

	if it.pos >= len(it.blockData) {
		if it.blockIndex >= len(it.r.blockIndex.Entries) {
			it.eof = true
			it.key, it.val = nil, nil
			return nil
		}
		var err error
		it.blockData, err = it.r.readBlock(it.r.blockIndex.Entries[it.blockIndex].Offset, it.blockBuf[:])
		if err != nil {
			it.eof = true
			it.key, it.val = nil, nil
			return err
		}
		it.blockIndex++
		it.pos = 0
	}

	if it.pos+8 > len(it.blockData) {
		it.eof = true
		return ErrCorruptSSTable
	}
	klen := binary.LittleEndian.Uint32(it.blockData[it.pos : it.pos+4])
	vlen := binary.LittleEndian.Uint32(it.blockData[it.pos+4 : it.pos+8])

	// security check
	if klen > maxSSTableKeySize {
		it.eof = true
		it.key, it.val = nil, nil
		return ErrCorruptSSTable
	}

	if vlen > maxSSTableValueSize {
		it.eof = true
		it.key, it.val = nil, nil
		return ErrCorruptSSTable
	}

	totalLen := int(klen) + int(vlen)
	expectedEnd := it.pos + 8 + totalLen
	if expectedEnd > len(it.blockData) {
		it.eof = true
		it.key, it.val = nil, nil
		return ErrCorruptSSTable
	}

	record := it.blockData[it.pos+8 : expectedEnd]
	it.key = record[:int(klen)]
	if vlen == 0 {
		it.val = nil
	} else {
		it.val = record[int(klen):]
	}

	// update position
	it.pos = expectedEnd

	return nil
}
