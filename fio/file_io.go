package fio

import "os"

// FileIO 标准文件IO
type FileIO struct {
	fd *os.File // 文件描述符
}

func NewFileIOManager(filename string) (*FileIO, error) {
	file, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, // 没有则创建，支持读写，只支持追加写入
		DataFilePerm,                      // 文件权限
	)
	if err != nil {
		return nil, err
	}

	return &FileIO{fd: file}, nil
}

func (fio *FileIO) Read(data []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(data, offset)
}

func (fio *FileIO) Write(data []byte) (int, error) {
	return fio.fd.Write(data)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

var _ IOManager = (*FileIO)(nil)
