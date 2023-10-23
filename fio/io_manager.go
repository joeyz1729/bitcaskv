package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFIO FileIOType = iota // FIO 标准文件IO
	MemoryMap                     // MMap 内存文件映射
)

// IOManager IO管理接口
type IOManager interface {
	Read(data []byte, offset int64) (int, error) // 从给定位置读取数据
	Write(data []byte) (int, error)              // 写入数据
	Sync() error                                 // 数据刷盘持久化
	Close() error                                // 关闭
	Size() (int64, error)                        // 获取文件大小
}

// NewIOManager 初始化
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
