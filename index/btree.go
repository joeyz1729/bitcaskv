package index

import (
	"bitcaskv/data"
	"github.com/google/btree"
	"sync"
)

// BTree 封装B树作为内存存储
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), // 控制B树的节点数量
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(item)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) (pos *data.LogRecordPos) {
	item := &Item{key: key}
	btreeItem := bt.tree.Get(item)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	item := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(item)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

var _ Indexer = (*BTree)(nil)
