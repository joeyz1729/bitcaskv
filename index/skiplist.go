package index

import (
	"bitcaskv/data"
	"bitcaskv/index/ds"
	"bytes"
	"sort"
	"sync"
)

type SkipList struct {
	skl  *ds.SkipList
	lock *sync.RWMutex
}

func NewSkipList() *SkipList {
	return &SkipList{
		skl:  ds.NewSkipList(),
		lock: new(sync.RWMutex),
	}
}

func (sl *SkipList) Size() int {
	return sl.skl.Size()
}
func (sl *SkipList) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	sl.lock.Lock()
	element := sl.skl.Put(key, pos)
	sl.lock.Unlock()
	if element == nil {
		return nil
	}
	return element.Value().(*data.LogRecordPos)
}

func (sl *SkipList) Get(key []byte) *data.LogRecordPos {
	element := sl.skl.Get(key)
	return element.Value().(*data.LogRecordPos)
}

func (sl *SkipList) Delete(key []byte) (*data.LogRecordPos, bool) {
	sl.lock.Lock()
	element := sl.skl.Remove(key)
	sl.lock.Unlock()
	if element == nil {
		return nil, false
	}
	return element.Value().(*data.LogRecordPos), true
}
func (sl *SkipList) Iterator(reverse bool) Iterator {
	if sl.skl == nil {
		return nil
	}
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	return newSkipListIterator(sl.skl, reverse)
}
func (sl *SkipList) Close() error {
	return sl.skl.Close()
}

var _ Indexer = (*SkipList)(nil)

type skipListIterator struct {
	curIndex int     // 当前遍历的下标位置
	reverse  bool    // 是否是反向遍历
	values   []*Item // key+位置索引信息
}

func newSkipListIterator(skl *ds.SkipList, reverse bool) *skipListIterator {
	values := make([]*Item, skl.Size())

	var idx, step int
	if reverse {
		idx = 0
		step = 1
	} else {
		idx = skl.Size() - 1
		step = -1
	}
	saveValue := func(e *ds.Element) bool {
		item := &Item{
			key: e.Key(),
			pos: e.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		idx += step
		return true
	}
	skl.Foreach(saveValue)

	return &skipListIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (ski *skipListIterator) Rewind() {
	ski.curIndex = 0
}
func (ski *skipListIterator) Seek(key []byte) {
	if ski.reverse {
		ski.curIndex = sort.Search(len(ski.values), func(i int) bool {
			return bytes.Compare(ski.values[i].key, key) <= 0
		})
	} else {
		ski.curIndex = sort.Search(len(ski.values), func(i int) bool {
			return bytes.Compare(ski.values[i].key, key) >= 0
		})
	}
}
func (ski *skipListIterator) Next() {
	ski.curIndex++
}
func (ski *skipListIterator) Valid() bool {
	return ski.curIndex < len(ski.values)
}
func (ski *skipListIterator) Key() []byte {
	return ski.values[ski.curIndex].key
}
func (ski *skipListIterator) Value() *data.LogRecordPos {
	return ski.values[ski.curIndex].pos

}
func (ski *skipListIterator) Close() {
	ski.values = nil
}

var _ Iterator = (*skipListIterator)(nil)
