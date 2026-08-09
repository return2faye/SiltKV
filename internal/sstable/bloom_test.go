package sstable

import (
	"fmt"
	"testing"
)

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	const n = 10_000
	bf := NewBloomFilter(n, 0.01)
	for i := 0; i < n; i++ {
		bf.Add([]byte(fmt.Sprintf("present-%05d", i)))
	}
	for i := 0; i < n; i++ {
		if !bf.MayContain([]byte(fmt.Sprintf("present-%05d", i))) {
			t.Fatalf("false negative for key %d", i)
		}
	}
	falsePositives := 0
	for i := 0; i < n; i++ {
		if bf.MayContain([]byte(fmt.Sprintf("missing-%05d", i))) {
			falsePositives++
		}
	}
	t.Logf("false positives: %d/%d (%.2f%%)", falsePositives, n, float64(falsePositives)*100/n)
	if falsePositives > n*3/100 {
		t.Fatalf("false-positive rate %.2f%% exceeds 3%%", float64(falsePositives)*100/n)
	}
}
