package data

// LogRecordPos 数据内存索引，描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}
