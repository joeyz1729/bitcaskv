package bitcaskv

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
