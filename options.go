package bitcaskv

// Options 数据库配置信息
type Options struct {
	DirPath      string // 数据库文件存放的位置
	DataFileSize int64

	SyncWrites bool // 	是否开启安全持久化
}
