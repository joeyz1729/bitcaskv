package data

import (
	"encoding/binary"
	"hash/crc32"
)

// LogRecordType 记录的状态，墓碑值记录是否已经被删除
type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
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

// TransactionRecord batch操作的记录信息
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 将数据记录编码，包括crc，type，key size， value size，用于写入数据文件
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化header数组
	header := make([]byte, maxLogRecordHeaderSize)

	// 添加type
	header[4] = logRecord.Type
	var index = 5

	// 添加变长的size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	var totalSize = index + len(logRecord.Key) + len(logRecord.Value)

	// 生成crc校验和并添加
	encBytes := make([]byte, totalSize)
	copy(encBytes[:index], header)
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	//fmt.Printf("header length: %d, header crc: %d\n", index, crc)

	return encBytes, int64(totalSize)
}

// decodeLogRecordHeader 对header进行解码，返回header解码后的结果和header的长度
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	// 获取crc和type
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	// 获取变长的size
	var index = 5
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	return header, int64(index)
}

// EncodeLogRecordPos 对 log record pos 进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}

// DecodeLogRecordPos 解码 LogRecordPos
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	//index += n
	//size, _ := binary.Varint(buf[index:])
	return &LogRecordPos{Fid: uint32(fileId), Offset: offset}
}
