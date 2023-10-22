package bitcaskv

import "os"

// Options 数据库配置信息
type Options struct {
	DirPath      string    // 数据库文件存放的位置
	DataFileSize int64     // 数据文件的大小
	IndexType    IndexType // 使用的内存索引类型
	SyncWrites   bool      // 是否开启写持久化
}

type IndexType uint8

const (
	TypeBTree IndexType = iota + 1
	TypeART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	IndexType:    TypeBTree,
	SyncWrites:   false,
}

type IteratorOptions struct {
	Prefix  []byte // 根据前缀迭代
	Reverse bool   // 指定迭代顺序
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  []byte{},
	Reverse: false,
}

// WriteBatchOptions 原子批量写入操作的选项
type WriteBatchOptions struct {
	MaxBatchNum uint // 一个批次中最大的操作量
	SyncWrites  bool // 提交事务的时候是否进行持久化

}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  false,
}
