package index

import (
	"bitcaskv/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(), // 控制B树的节点数量
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

func (art *AdaptiveRadixTree) Get(key []byte) (pos *data.LogRecordPos) {
	art.lock.Lock()
	val, found := art.tree.Search(key)
	art.lock.Unlock()
	if !found {
		return nil
	}
	return val.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	_, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	return deleted
}

func (art *AdaptiveRadixTree) Size() int {
	return art.tree.Size()
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art.tree == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}

var _ Indexer = (*AdaptiveRadixTree)(nil)

// ArtIterator 索引迭代器
type ArtIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key+位置索引信息
}

func newArtIterator(tree goart.Tree, reverse bool) *ArtIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues)

	return &ArtIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (ai *ArtIterator) Rewind() {
	ai.currIndex = 0
}

func (ai *ArtIterator) Seek(key []byte) {
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

func (ai *ArtIterator) Next() {
	ai.currIndex += 1
}

func (ai *ArtIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

func (ai *ArtIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

func (ai *ArtIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

func (ai *ArtIterator) Close() {
	ai.values = nil
}
