package utils

import (
	"reflect"
	"testing"
)

func TestCopyBytes(t *testing.T) {
	original := []byte("test data")
	copied := CopyBytes(original)

	// Verify content is the same
	if string(copied) != string(original) {
		t.Errorf("Expected %s, got %s", string(original), string(copied))
	}

	// Verify it's a different slice (not the same underlying array)
	if len(copied) > 0 && len(original) > 0 {
		copied[0] = 'X'
		if original[0] == 'X' {
			t.Error("CopyBytes should create a new slice, not share the underlying array")
		}
	}
}

func TestCopyBytesNil(t *testing.T) {
	var nilSlice []byte
	copied := CopyBytes(nilSlice)

	if copied != nil {
		t.Error("CopyBytes(nil) should return nil")
	}
}

func TestCopyBytesEmpty(t *testing.T) {
	empty := []byte{}
	copied := CopyBytes(empty)

	if len(copied) != 0 {
		t.Errorf("Expected empty slice, got length %d", len(copied))
	}

	// Verify it's a new slice
	if !reflect.DeepEqual(copied, empty) {
		t.Error("Empty slice copy should be equal")
	}
}

func TestCopyBytesModification(t *testing.T) {
	original := []byte("hello")
	copied := CopyBytes(original)

	// Modify original
	original[0] = 'H'

	// Copied should be unchanged
	if copied[0] != 'h' {
		t.Error("Modifying original should not affect copied slice")
	}
}
