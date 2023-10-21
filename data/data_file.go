package data

import "bitcaskv/fio"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32 // 文件id
	WriteOff  int64  // 当前写入的偏移
	IoManager fio.IOManager
}

// OpenDataFile 打开数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	//TODO
	return nil, nil
}

func (dataFile *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	//TODO
	return nil, nil
}

func (dataFile *DataFile) Write(buf []byte) error {
	//TODO
	return nil
}

func (dataFile *DataFile) Sync() error {
	//TODO
	return nil
}
