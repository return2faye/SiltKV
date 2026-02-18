package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/return2faye/SiltKV/internal/memtable"
	"github.com/return2faye/SiltKV/internal/utils"
)

const (
	maxSSTableKeySize   = 1 << 20  // 1MB
	maxSSTableValueSize = 4 << 20  // 4MB
	maxSSTableFileSize  = 64 << 20 // 64MB - maximum size for a single SSTable file
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
	currentBlock    []byte       // Current block buffer being written
	blockOffset     int64        // Starting offset of the current block
	firstKeyInBlock []byte       // First key in the current block
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
	}, nil
}

// flushCurrentBlock writes the current block to the file and adds it to the Block Index
func (w *Writer) flushCurrentBlock() error {
	if len(w.currentBlock) == 0 {
		return nil
	}

	// Record the starting offset of the block
	blockOffset := w.fileSize

	// Write the block to the file
	if _, err := w.file.Write(w.currentBlock); err != nil {
		return err
	}

	// If there's a first key, add it to the Block Index
	if w.firstKeyInBlock != nil {
		w.blockIndex.Add(w.firstKeyInBlock, blockOffset)
	}

	// Update file size
	w.fileSize += int64(len(w.currentBlock))

	// Reset current block (preserve capacity)
	w.currentBlock = w.currentBlock[:0]
	w.firstKeyInBlock = nil
	w.blockOffset = w.fileSize

	return nil
}

// writeRecordToBlock writes a record to the current block
// Returns true if the block is full and needs to be flushed
func (w *Writer) writeRecordToBlock(key, value []byte) (bool, error) {
	klen := uint32(len(key))
	vlen := uint32(len(value))
	recordSize := 8 + len(key) + len(value)

	// If this is the first key in the block, save it
	if w.firstKeyInBlock == nil {
		w.firstKeyInBlock = utils.CopyBytes(key)
	}

	// Check if the record can fit in the current block
	if len(w.currentBlock)+recordSize > BlockSize && len(w.currentBlock) > 0 {
		// Block is full, need to flush
		if err := w.flushCurrentBlock(); err != nil {
			return false, err
		}
		return true, nil
	}

	// Write the record to the block buffer
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], klen)
	binary.LittleEndian.PutUint32(header[4:8], vlen)

	w.currentBlock = append(w.currentBlock, header...)
	w.currentBlock = append(w.currentBlock, key...)
	w.currentBlock = append(w.currentBlock, value...)

	return false, nil
}

func (w *Writer) Close() error {
	if w.file == nil {
		return nil
	}

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

	// 3. Write Bloom Filter
	if w.bloomFilter == nil {
		// If there's no data, create an empty Bloom Filter
		w.bloomFilter = NewBloomFilter(1, 0.01)
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

	err := w.file.Close()
	w.file = nil
	return err
}

// WriteFromIterator writes all key-value pairs from the iterator to the SSTable
// Data will be organized into multiple blocks, and a Bloom Filter and sparse index will be built
func (w *Writer) WriteFromIterator(it *memtable.SLIterator) error {
	if w.file == nil {
		return os.ErrInvalid
	}

	// Initialize Bloom Filter (estimate capacity)
	if w.bloomFilter == nil {
		w.bloomFilter = NewBloomFilter(10000, 0.01)
	}

	// Iterate through the iterator and write data
	for it.Valid() {
		key := it.Key()
		val := it.Value()

		// Add to Bloom Filter
		w.bloomFilter.Add(key)

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

	// Initialize Bloom Filter (if not already initialized)
	if w.bloomFilter == nil {
		w.bloomFilter = NewBloomFilter(1000, 0.01)
	}

	// Add to Bloom Filter
	w.bloomFilter.Add(key)

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

	if r.fileSize < 32 {
		// Old format or empty file - use legacy mode
		r.footer = nil
		r.blockIndex = nil
		r.bloomFilter = nil
		r.initialized = true
		return nil
	}

	// Read footer (last 32 bytes)
	footerData := make([]byte, 32)
	if _, err := r.file.ReadAt(footerData, r.fileSize-32); err != nil {
		// Old format - use legacy mode
		r.footer = nil
		r.blockIndex = nil
		r.bloomFilter = nil
		r.initialized = true
		return nil
	}

	footer, err := DeserializeFooter(footerData)
	if err != nil {
		// Old format - use legacy mode
		r.footer = nil
		r.blockIndex = nil
		r.bloomFilter = nil
		r.initialized = true
		return nil
	}
	r.footer = footer

	// Validate footer offsets
	if footer.BlockIndexOffset < 0 || footer.BlockIndexSize < 0 ||
		footer.BloomFilterOffset < 0 || footer.BlockIndexOffset > r.fileSize ||
		footer.BloomFilterOffset > r.fileSize {
		// Invalid footer - use legacy mode
		r.footer = nil
		r.blockIndex = nil
		r.bloomFilter = nil
		r.initialized = true
		return nil
	}

	// Read block index
	if footer.BlockIndexSize > 0 && footer.BlockIndexOffset+footer.BlockIndexSize <= r.fileSize {
		blockIndexData := make([]byte, footer.BlockIndexSize)
		if _, err := r.file.ReadAt(blockIndexData, footer.BlockIndexOffset); err != nil {
			// Old format - use legacy mode
			r.footer = nil
			r.blockIndex = nil
			r.bloomFilter = nil
			r.initialized = true
			return nil
		}

		blockIndex, err := DeserializeBlockIndex(blockIndexData)
		if err != nil {
			// Old format - use legacy mode
			r.footer = nil
			r.blockIndex = nil
			r.bloomFilter = nil
			r.initialized = true
			return nil
		}
		r.blockIndex = blockIndex
	}

	// Read bloom filter
	if footer.BloomFilterOffset < footer.BlockIndexOffset {
		bloomFilterSize := footer.BlockIndexOffset - footer.BloomFilterOffset
		if bloomFilterSize > 0 && bloomFilterSize < 1024*1024 { // Sanity check: max 1MB
			bloomFilterData := make([]byte, bloomFilterSize)
			if _, err := r.file.ReadAt(bloomFilterData, footer.BloomFilterOffset); err != nil {
				// Old format - use legacy mode
				r.footer = nil
				r.blockIndex = nil
				r.bloomFilter = nil
				r.initialized = true
				return nil
			}

			bloomFilter, err := LoadBloomFilter(bloomFilterData)
			if err != nil {
				// Old format - use legacy mode
				r.footer = nil
				r.blockIndex = nil
				r.bloomFilter = nil
				r.initialized = true
				return nil
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

	// Legacy format: use linear scan
	if r.footer == nil || r.blockIndex == nil {
		return r.getLegacy(key)
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

// getLegacy implements the old linear scan method (for backward compatibility)
func (r *Reader) getLegacy(key []byte) ([]byte, bool, error) {
	it := r.NewIterator()

	// Move to the first data
	if err := it.Next(); err != nil {
		return nil, false, err
	}

	// Linear scan
	for it.Valid() {
		cmp := bytes.Compare(it.Key(), key)
		if cmp == 0 {
			val := utils.CopyBytes(it.Value())
			return val, true, nil
		}
		// Past the target key, terminate
		if cmp > 0 {
			return nil, false, nil
		}

		if err := it.Next(); err != nil {
			return nil, false, err
		}
	}

	return nil, false, nil
}

// searchInBlock searches for a key within the specified block
func (r *Reader) searchInBlock(key []byte, blockOffset int64) ([]byte, bool, error) {
	// Determine the end position of the block (start of next block or end of data section)
	// Data section ends at the start of the Block Index (not the Bloom Filter).
	// Layout: [data blocks][block index][bloom filter][footer]
	blockEnd := r.footer.BlockIndexOffset
	if len(r.blockIndex.Entries) > 0 {
		// Find the offset of the next block
		for _, entry := range r.blockIndex.Entries {
			if entry.Offset > blockOffset {
				blockEnd = entry.Offset
				break
			}
		}
	}

	blockSize := blockEnd - blockOffset
	if blockSize <= 0 {
		return nil, false, nil
	}

	// Read the entire block
	blockData := make([]byte, blockSize)
	if _, err := r.file.ReadAt(blockData, blockOffset); err != nil {
		return nil, false, err
	}

	// Parse the block and search for the key
	pos := int64(0)
	for pos < blockSize {
		if pos+8 > blockSize {
			break
		}

		// Read header
		klen := binary.LittleEndian.Uint32(blockData[pos : pos+4])
		vlen := binary.LittleEndian.Uint32(blockData[pos+4 : pos+8])

		if klen > maxSSTableKeySize || vlen > maxSSTableValueSize {
			return nil, false, io.ErrUnexpectedEOF
		}

		totalLen := int64(klen) + int64(vlen)
		if pos+8+totalLen > blockSize {
			break
		}

		recordKey := blockData[pos+8 : pos+8+int64(klen)]
		cmp := bytes.Compare(recordKey, key)

		if cmp == 0 {
			// Found it!
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
	r       *Reader
	pos     int64 // offset in file
	dataEnd int64 // End position of data section (before Bloom Filter)
	key     []byte
	val     []byte
	eof     bool
}

func (r *Reader) NewIterator() *Iterator {
	// Initialize (if not already initialized)
	if !r.initialized {
		r.initialize()
	}

	// Determine the end position of the data section
	dataEnd := r.fileSize
	if r.footer != nil && r.footer.BlockIndexOffset > 0 {
		// New format: data ends before Block Index (not Bloom Filter).
		// Layout: [data blocks][block index][bloom filter][footer]
		// Ensure we don't read into metadata/footer.
		if r.footer.BlockIndexOffset <= r.fileSize-32 {
			dataEnd = r.footer.BlockIndexOffset
		} else if r.fileSize > 32 {
			dataEnd = r.fileSize - 32
		}
	}

	return &Iterator{
		r:       r,
		pos:     0,
		dataEnd: dataEnd,
	}
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

	// Check if we've reached the end of the data section
	// Note: use >= instead of >, because pos is the next position to read
	if it.pos >= it.dataEnd {
		it.eof = true
		it.key, it.val = nil, nil
		return nil
	}

	// read header
	header := make([]byte, 8)

	// no header corruption
	n, err := it.r.file.ReadAt(header, it.pos)
	if err == io.EOF && n == 0 {
		it.eof = true
		it.key, it.val = nil, nil
		return nil
	}

	// other problems
	if err != nil && err != io.EOF {
		return err
	}

	// header incomplete
	if n < 8 {
		it.eof = true
		it.key, it.val = nil, nil
		return nil
	}

	klen := binary.LittleEndian.Uint32(header[0:4])
	vlen := binary.LittleEndian.Uint32(header[4:8])

	// security check
	if klen > maxSSTableKeySize {
		it.eof = true
		it.key, it.val = nil, nil
		return nil
	}

	if vlen > maxSSTableValueSize {
		it.eof = true
		it.key, it.val = nil, nil
		return nil
	}

	totalLen := int64(klen) + int64(vlen)
	if totalLen < 0 {
		it.eof = true
		it.key, it.val = nil, nil
		return nil
	}

	expectedEnd := it.pos + 8 + totalLen
	if expectedEnd > it.dataEnd {
		// Exceeded data section, reached end of file
		it.eof = true
		it.key, it.val = nil, nil
		return nil
	}

	buf := make([]byte, totalLen)
	n, err = it.r.file.ReadAt(buf, it.pos+8)
	if err != nil && err != io.EOF {
		return err
	}

	if int64(n) < totalLen {
		it.eof = true
		it.key, it.val = nil, nil
		return nil
	}

	it.key = buf[:klen]
	it.val = buf[klen:]

	// update position
	it.pos += 8 + totalLen

	return nil
}
