package bitcaskv

import "os"

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	// 累计写到多少字节后进行持久化
	BytesPerSync uint

	// 索引类型
	IndexType IndexerType

	// 启动时是否使用 MMap 加载数据
	MMapAtStartup bool

	//	数据文件合并的阈值
	DataFileMergeRatio float32
}

type IndexerType uint8

const (
	TypeBTree IndexerType = iota + 1
	TypeART
	TypeBPlusTree
)

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024,
	IndexType:          TypeBTree,
	SyncWrites:         false,
	BytesPerSync:       0,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
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
