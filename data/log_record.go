package data

// LogRecordType 记录的状态，墓碑值记录是否已经被删除
type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord 写入到数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引，描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

// EncodeLogRecord 将数据记录编码
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
