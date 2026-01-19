package utils

// deep copy of bytes slice
// Depensive Copying: not modify original array
func CopyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}

	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}