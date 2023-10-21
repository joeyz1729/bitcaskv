package index

import (
	"bitcaskv/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool // 向索引存储key的磁盘位置信息
	Get(key []byte) *data.LogRecordPos           // 获取内存索引中存储的磁盘位置信息
	Delete(key []byte) bool                      // 删除某个key的索引
}

type IndexerType int8

const (
	TypeBTree IndexerType = iota + 1
	TypeART
)

// NewIndexer 根据类型初始化索引
func NewIndexer(typ IndexerType) Indexer {
	switch typ {
	case TypeBTree:
		return NewBTree()
	case TypeART:
		//TODO
		return nil
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
