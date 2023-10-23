package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO，内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(filename string) (*MMap, error) {
	if _, err := os.OpenFile(filename, os.O_CREATE, DataFilePerm); err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{
		readerAt,
	}, nil
}

func (mmap *MMap) Read(data []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(data, offset)
}

// Write 只使用读方法，不需要Write
func (mmap *MMap) Write(data []byte) (int, error) {
	panic("not implemented")
}

func (mmap *MMap) Sync() error {
	panic("not implemented")
}
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}

var _ IOManager = (*MMap)(nil)
