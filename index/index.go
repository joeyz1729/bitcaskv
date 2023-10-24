package index

import (
	"bitcaskv/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	Size() int                                                 // 返回索引中一共有多少条数据
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos // 向索引存储key的磁盘位置信息
	Get(key []byte) *data.LogRecordPos                         // 获取内存索引中存储的磁盘位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)              // 删除某个key的索引
	Iterator(reverse bool) Iterator                            // 返回迭代器
	Close() error
}

type IndexerType int8

const (
	TypeBTree IndexerType = iota + 1
	TypeART
	TypeBPlusTree
	TypeSkipList
	TypeSkipList1
)

// NewIndexer 根据类型初始化索引
func NewIndexer(typ IndexerType, dirPath string, syncWrites bool) Indexer {
	switch typ {
	case TypeBTree:
		return NewBTree()
	case TypeART:
		return NewART()
	case TypeBPlusTree:
		return NewBPlusTree(dirPath, syncWrites)
	case TypeSkipList:
		return NewSkipList()
	case TypeSkipList1:
		return NewSkipList1()
	default:
		panic("unsupported indexer type")
	}
}

// Item 存储在BTree中的对象，实现Less方法
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (item *Item) Less(bi btree.Item) bool {
	return bytes.Compare(item.key, bi.(*Item).key) == -1
}

var _ btree.Item = (*Item)(nil)

// Iterator 通用索引迭代器
type Iterator interface {
	Rewind()                   // 重新回到第一个元素
	Seek(key []byte)           // 查找第一个大于（小于）等于key的值
	Next()                     // 查找下一个值
	Valid() bool               // 检查迭代是否结束
	Key() []byte               // 获取key的值
	Value() *data.LogRecordPos // 获取当前遍历位置的value
	Close()                    // 关闭迭代器
}
