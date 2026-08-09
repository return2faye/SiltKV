package sstable

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
)

const bloomDoubleHashFlag = uint32(1 << 31)

// BloomFilter is a probabilistic data structure used to test whether an element is a member of a set.
// False positives are possible, but false negatives are not.
// This allows us to quickly skip SSTables that definitely don't contain a key.
type BloomFilter struct {
	bits      []byte // bit array
	bitCount  uint32 // number of bits in the filter
	hashCount uint32
	legacy    bool
}

type bloomHash struct {
	h1 uint32
	h2 uint32
}

// NewBloomFilter creates a new Bloom filter with the given capacity and false positive rate.
// capacity: expected number of elements
// falsePositiveRate: desired false positive rate (e.g., 0.01 for 1%)
func NewBloomFilter(capacity uint32, falsePositiveRate float64) *BloomFilter {
	if capacity == 0 {
		capacity = 1
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}
	// Calculate optimal number of bits: m = -n * ln(p) / (ln(2)^2)
	// where n is capacity, p is false positive rate
	bitCount := uint32(-float64(capacity) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2))

	// Round up to nearest byte
	byteCount := (bitCount + 7) / 8
	bitCount = byteCount * 8

	// Calculate optimal number of hash functions: k = (m/n) * ln(2)
	hashCount := uint32(float64(bitCount) / float64(capacity) * math.Ln2)
	if hashCount < 1 {
		hashCount = 1
	}
	if hashCount > 10 {
		hashCount = 10 // Cap at 10 hash functions
	}

	return &BloomFilter{
		bits:      make([]byte, byteCount),
		bitCount:  bitCount,
		hashCount: hashCount,
	}
}

func fnv32a(key []byte) uint32 {
	h := uint32(2166136261)
	for _, b := range key {
		h = (h ^ uint32(b)) * 16777619
	}
	return h
}

func hashBloomKey(key []byte) bloomHash {
	return bloomHash{h1: fnv32a(key), h2: crc32.ChecksumIEEE(key) | 1}
}

// Add adds a key to the Bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	bf.addHash(hashBloomKey(key))
}

func (bf *BloomFilter) addHash(hash bloomHash) {
	for i := uint32(0); i < bf.hashCount; i++ {
		hashValue := hash.h1 + i*hash.h2
		bitIndex := hashValue % bf.bitCount
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8
		bf.bits[byteIndex] |= 1 << bitOffset
	}
}

// MayContain checks if the key might be in the filter.
// Returns true if the key might be present (could be false positive).
// Returns false if the key is definitely not present.
func (bf *BloomFilter) MayContain(key []byte) bool {
	hash := hashBloomKey(key)
	count := bf.hashCount
	if bf.legacy {
		count = 1 // old files stored the same FNV hash count times
	}
	for i := uint32(0); i < count; i++ {
		hashValue := hash.h1 + i*hash.h2
		bitIndex := hashValue % bf.bitCount
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8
		if (bf.bits[byteIndex] & (1 << bitOffset)) == 0 {
			return false
		}
	}
	return true
}

// Bytes returns the serialized Bloom filter.
func (bf *BloomFilter) Bytes() []byte {
	// Format: [bitCount(4)][hashCount(4)][bits...]
	result := make([]byte, 8+len(bf.bits))
	binary.LittleEndian.PutUint32(result[0:4], bf.bitCount)
	binary.LittleEndian.PutUint32(result[4:8], bf.hashCount|bloomDoubleHashFlag)
	copy(result[8:], bf.bits)
	return result
}

// LoadBloomFilter loads a Bloom filter from serialized bytes.
func LoadBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 8 {
		return nil, io.ErrUnexpectedEOF
	}

	bitCount := binary.LittleEndian.Uint32(data[0:4])
	encodedHashCount := binary.LittleEndian.Uint32(data[4:8])
	legacy := encodedHashCount&bloomDoubleHashFlag == 0
	hashCount := encodedHashCount &^ bloomDoubleHashFlag
	if bitCount == 0 || hashCount == 0 || hashCount > 10 {
		return nil, io.ErrUnexpectedEOF
	}

	expectedSize := 8 + int(bitCount+7)/8
	if len(data) < expectedSize {
		return nil, io.ErrUnexpectedEOF
	}

	bits := make([]byte, (bitCount+7)/8)
	copy(bits, data[8:8+(bitCount+7)/8])

	return &BloomFilter{
		bits:      bits,
		bitCount:  bitCount,
		hashCount: hashCount,
		legacy:    legacy,
	}, nil
}
