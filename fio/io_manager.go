package fio

const DataFilePerm = 0644

// IOManager IO管理接口
type IOManager interface {
	Read([]byte, int64) (int, error) // 从给定位置读取数据
	Write([]byte) (int, error)       // 写入数据
	Sync() error                     // 数据刷盘持久化
	Close() error                    // 关闭
}
