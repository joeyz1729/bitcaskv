package data

import "encoding/binary"

// LogRecordType 记录的状态，墓碑值记录是否已经被删除
type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

const (
	maxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32*2 //	crc, type, keySize, valueSize 4 + 1 + 5 + 5
)

// LogRecord 写入到数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecord 头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值， 4
	recordType LogRecordType // LogRecord类型, 1
	keySize    uint32        // 变长，5
	valueSize  uint32        // 变长，5
}

// LogRecordPos 数据内存索引，描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

// EncodeLogRecord 将数据记录编码
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	//TODO
	return nil, 0
}

// decodeLogRecordHeader 对header进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	//TODO
	return nil, 0
}
