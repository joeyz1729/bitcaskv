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

type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
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
