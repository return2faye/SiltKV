package sstable

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"io"
)

// BloomFilter is a probabilistic data structure used to test whether an element is a member of a set.
// False positives are possible, but false negatives are not.
// This allows us to quickly skip SSTables that definitely don't contain a key.
type BloomFilter struct {
	bits     []byte // bit array
	bitCount uint32 // number of bits in the filter
	hashFunc []hash.Hash32
}

// NewBloomFilter creates a new Bloom filter with the given capacity and false positive rate.
// capacity: expected number of elements
// falsePositiveRate: desired false positive rate (e.g., 0.01 for 1%)
func NewBloomFilter(capacity uint32, falsePositiveRate float64) *BloomFilter {
	// Calculate optimal number of bits: m = -n * ln(p) / (ln(2)^2)
	// where n is capacity, p is false positive rate
	bitCount := uint32(float64(capacity) * (-1.0 * log(falsePositiveRate)) / (log(2.0) * log(2.0)))

	// Round up to nearest byte
	byteCount := (bitCount + 7) / 8
	bitCount = byteCount * 8

	// Calculate optimal number of hash functions: k = (m/n) * ln(2)
	hashCount := int((float64(bitCount) / float64(capacity)) * log(2.0))
	if hashCount < 1 {
		hashCount = 1
	}
	if hashCount > 10 {
		hashCount = 10 // Cap at 10 hash functions
	}

	// Create hash functions
	hashFuncs := make([]hash.Hash32, hashCount)
	for i := 0; i < hashCount; i++ {
		hashFuncs[i] = fnv.New32a()
	}

	return &BloomFilter{
		bits:     make([]byte, byteCount),
		bitCount: bitCount,
		hashFunc: hashFuncs,
	}
}

// Add adds a key to the Bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	for _, h := range bf.hashFunc {
		h.Reset()
		h.Write(key)
		hashValue := h.Sum32()
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
	for _, h := range bf.hashFunc {
		h.Reset()
		h.Write(key)
		hashValue := h.Sum32()
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
	binary.LittleEndian.PutUint32(result[4:8], uint32(len(bf.hashFunc)))
	copy(result[8:], bf.bits)
	return result
}

// LoadBloomFilter loads a Bloom filter from serialized bytes.
func LoadBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 8 {
		return nil, io.ErrUnexpectedEOF
	}

	bitCount := binary.LittleEndian.Uint32(data[0:4])
	hashCount := binary.LittleEndian.Uint32(data[4:8])

	expectedSize := 8 + int(bitCount+7)/8
	if len(data) < expectedSize {
		return nil, io.ErrUnexpectedEOF
	}

	bits := make([]byte, (bitCount+7)/8)
	copy(bits, data[8:8+(bitCount+7)/8])

	// Recreate hash functions
	hashFuncs := make([]hash.Hash32, hashCount)
	for i := uint32(0); i < hashCount; i++ {
		hashFuncs[i] = fnv.New32a()
	}

	return &BloomFilter{
		bits:     bits,
		bitCount: bitCount,
		hashFunc: hashFuncs,
	}, nil
}

// log calculates natural logarithm (approximation using Taylor series)
func log(x float64) float64 {
	if x <= 0 {
		return 0
	}
	// Simple approximation: ln(x) ≈ (x-1) - (x-1)^2/2 + (x-1)^3/3 - ...
	// For better accuracy, we use a more precise approximation
	// ln(x) ≈ 2 * ((x-1)/(x+1)) * (1 + ((x-1)/(x+1))^2/3 + ...)
	if x < 0.5 {
		return -log(1.0 / x)
	}
	if x > 2.0 {
		return log(x/2.0) + log(2.0)
	}
	// For x in [0.5, 2.0], use approximation
	z := (x - 1.0) / (x + 1.0)
	result := 2.0 * z
	z2 := z * z
	term := z
	for i := 1; i < 10; i++ {
		term *= z2
		result += 2.0 * term / float64(2*i+1)
	}
	return result
}
