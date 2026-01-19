package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/macz/SiltKV/internal/memtable"
	"github.com/macz/SiltKV/internal/utils"
)

const (
	maxSSTableKeySize   = 1 << 20  // 1MB
	maxSSTableValueSize = 10 << 20 // 10MB
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

// format: [klen(4)][vlen(4)][key][value]
func (w *Writer) WriteFromIterator(it *memtable.SLIterator) error {
	if w.file == nil {
		return os.ErrInvalid
	}

	for it.Valid() {
		key := it.Key()
		val := it.Value()

		klen := uint32(len(key))
		vlen := uint32(len(val))

		// write header: Length Prefix
		header := make([]byte, 8)
		binary.LittleEndian.PutUint32(header[0:4], klen)
		binary.LittleEndian.PutUint32(header[4:8], vlen)

		if _, err := w.file.Write(header); err != nil {
			return err
		}

		// write key
		if _, err := w.file.Write(key); err != nil {
			return err
		}

		// write value
		if _, err := w.file.Write(val); err != nil {
			return err
		}

		it.Next()
	}

	return nil
}

// Read from SSTable files
type Reader struct {
	file     *os.File
	fileSize int64
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

	return &Reader{
		file:     f,
		fileSize: stat.Size(),
	}, nil
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

	it := r.NewIterator()

	// move it to first data
	if err := it.Next(); err != nil {
		return nil, false, err
	}

	// v1: linear Scan
	for it.Valid() {
		cmp := bytes.Compare(it.Key(), key)
		if cmp == 0 {
			val := utils.CopyBytes(it.Value())
			return val, true, nil
		}
		// exceed target key, terminate
		if cmp > 0 {
			return nil, false, nil
		}

		if err := it.Next(); err != nil {
			return nil, false, err
		}
	}

	return nil, false, nil
}

type Iterator struct {
	r   *Reader
	pos int64 // offset in file
	key []byte
	val []byte
	eof bool
}

func (r *Reader) NewIterator() *Iterator {
	return &Iterator{
		r:   r,
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
	if it.eof {
		return nil
	}
	if it.r == nil || it.r.file == nil {
		return os.ErrInvalid
	}

	if it.pos+8 > it.r.fileSize {
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
		return io.ErrUnexpectedEOF
	}

	if vlen > maxSSTableValueSize {
		return io.ErrUnexpectedEOF
	}

	totalLen := int64(klen) + int64(vlen)
	if totalLen < 0 {
		return io.ErrUnexpectedEOF
	}

	expectedEnd := it.pos + 8 + totalLen
	if expectedEnd > it.r.fileSize {
		return io.ErrUnexpectedEOF
	}

	buf := make([]byte, totalLen)
	n, err = it.r.file.ReadAt(buf, it.pos+8)
	if err != nil && err != io.EOF {
		return err
	}

	if int64(n) < totalLen {
		return io.ErrUnexpectedEOF
	}

	it.key = buf[:klen]
	it.val = buf[klen:]

	// update position
	it.pos += 8 + totalLen

	return nil
}
