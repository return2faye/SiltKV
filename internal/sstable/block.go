package sstable

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/return2faye/SiltKV/internal/utils"
)

const (
	// BlockSize is the target size for each data block (4KB)
	BlockSize = 4 * 1024
	// MagicNumber is used to identify valid SSTable files
	MagicNumber = 0x53494C544B56 // "SILTKV" in ASCII
)

// BlockIndexEntry represents an entry in the block index.
// It stores the first key of a block and the offset where the block starts.
type BlockIndexEntry struct {
	FirstKey []byte // First key in the block
	Offset   int64  // Offset of the block in the file
}

// BlockIndex is a sparse index that maps block first keys to block offsets.
type BlockIndex struct {
	Entries []BlockIndexEntry
}

// Add adds a new entry to the block index.
func (bi *BlockIndex) Add(firstKey []byte, offset int64) {
	bi.Entries = append(bi.Entries, BlockIndexEntry{
		FirstKey: utils.CopyBytes(firstKey),
		Offset:   offset,
	})
}

// FindBlock finds the block that might contain the given key.
// Returns the offset of the block, or -1 if no block could contain the key.
func (bi *BlockIndex) FindBlock(key []byte) int64 {
	// Binary search for the block
	// We want the last block whose first key <= target key
	left, right := 0, len(bi.Entries)-1
	result := int64(-1)

	for left <= right {
		mid := (left + right) / 2
		cmp := bytes.Compare(bi.Entries[mid].FirstKey, key)
		if cmp <= 0 {
			// This block might contain the key
			result = bi.Entries[mid].Offset
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return result
}

// Serialize serializes the block index to bytes.
// Format: [entryCount(4)][entry1: keyLen(4) + key + offset(8)][entry2: ...]
func (bi *BlockIndex) Serialize() []byte {
	var buf bytes.Buffer

	// Write entry count
	count := uint32(len(bi.Entries))
	binary.Write(&buf, binary.LittleEndian, count)

	// Write each entry
	for _, entry := range bi.Entries {
		keyLen := uint32(len(entry.FirstKey))
		binary.Write(&buf, binary.LittleEndian, keyLen)
		buf.Write(entry.FirstKey)
		binary.Write(&buf, binary.LittleEndian, entry.Offset)
	}

	return buf.Bytes()
}

// DeserializeBlockIndex deserializes a block index from bytes.
func DeserializeBlockIndex(data []byte) (*BlockIndex, error) {
	if len(data) < 4 {
		return nil, io.ErrUnexpectedEOF
	}

	reader := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	index := &BlockIndex{
		Entries: make([]BlockIndexEntry, 0, count),
	}

	for i := uint32(0); i < count; i++ {
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}

		if keyLen > maxSSTableKeySize {
			return nil, io.ErrUnexpectedEOF
		}

		key := make([]byte, keyLen)
		if n, err := reader.Read(key); err != nil || uint32(n) != keyLen {
			return nil, io.ErrUnexpectedEOF
		}

		var offset int64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			return nil, err
		}

		index.Entries = append(index.Entries, BlockIndexEntry{
			FirstKey: key,
			Offset:   offset,
		})
	}

	return index, nil
}

// Footer contains metadata at the end of an SSTable file.
type Footer struct {
	BloomFilterOffset int64 // Offset of bloom filter section
	BlockIndexOffset  int64 // Offset of block index section
	BlockIndexSize    int64 // Size of block index section
	MagicNumber       int64 // Magic number to verify file format
}

// Serialize serializes the footer to bytes (32 bytes total).
func (f *Footer) Serialize() []byte {
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(f.BloomFilterOffset))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(f.BlockIndexOffset))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(f.BlockIndexSize))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(f.MagicNumber))
	return buf
}

// DeserializeFooter deserializes a footer from bytes.
func DeserializeFooter(data []byte) (*Footer, error) {
	if len(data) < 32 {
		return nil, io.ErrUnexpectedEOF
	}

	footer := &Footer{
		BloomFilterOffset: int64(binary.LittleEndian.Uint64(data[0:8])),
		BlockIndexOffset:  int64(binary.LittleEndian.Uint64(data[8:16])),
		BlockIndexSize:    int64(binary.LittleEndian.Uint64(data[16:24])),
		MagicNumber:       int64(binary.LittleEndian.Uint64(data[24:32])),
	}

	// Verify magic number
	if footer.MagicNumber != MagicNumber {
		return nil, io.ErrUnexpectedEOF
	}

	return footer, nil
}
