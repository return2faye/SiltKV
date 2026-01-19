package sstable

import (
	"encoding/binary"
	"os"

	"github.com/macz/SiltKV/internal/memtable"
)

// abstraction of SSTable
// read single .sst file
type Table struct {
	file *os.File
	path string
}

// flush memtable into SSTable file
type Writer struct {
	file *os.File
}

func NewWriter(path string) (*Writer, error) {
	// SSTable is immutable, wo don't append
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &Writer{file: f}, nil
}

func (w *Writer) Close() error {
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *Writer) WriteFromIterator(it *memtable.SLIterator) error {
	if w.file == nil {
		return os.ErrInvalid
	}

	for it.Valid() {
		key := it.Key()
		val := it.Value()

		klen := uint32((len(key)))
		vlen := uint32(len(val))

		// write header: Length Prefix
		header := make([]byte, 8)
		binary.LittleEndian.PutUint32(header[0:4], klen)
		binary.LittleEndian.PutUint32(header[4:8], vlen)

		if _, err := w.file.Write(header); err != nil {
			return nil
		}

		// write key
		if _, err := w.file.Write(key); err != nil {
			return nil
		}

		// write value
		if _, err := w.file.Write(val); err != nil {
			return nil
		}

		it.Next()
	}

	return nil
}

// Read from SSTable files
type Reader struct {
	file *os.File
}

func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &Reader{file: f}, nil
}

func (r *Reader) Close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

type Iterator struct {
	r *Reader
	pos int64 // offset in file
	key []byte
	val []byte
	eof bool
}

func (r *Reader) NewIterator() *Iterator {
	return &Iterator{
		r: r,
		pos: 0,
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
	return nil
}
