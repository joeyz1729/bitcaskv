package index

import (
	"bitcaskv/data"
	"bytes"
	"github.com/huandu/skiplist"
	"sort"
	"sync"
)

type SkipList1 struct {
	skl  *skiplist.SkipList
	lock *sync.RWMutex
}

func NewSkipList1() *SkipList1 {
	return &SkipList1{
		skl:  skiplist.New(skiplist.Bytes),
		lock: new(sync.RWMutex),
	}
}

func (sl *SkipList1) Size() int {
	return sl.skl.Len()
}
func (sl *SkipList1) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	sl.lock.Lock()
	element := sl.skl.Get(key)
	if element != nil {
		oldPos := element.Value.(*data.LogRecordPos)
		element.Value = pos
		sl.lock.Unlock()
		return oldPos
	}
	_ = sl.skl.Set(key, pos)
	sl.lock.Unlock()
	return nil
}

func (sl *SkipList1) Get(key []byte) *data.LogRecordPos {
	element := sl.skl.Get(key)
	if element == nil {
		return nil
	}
	return element.Value.(*data.LogRecordPos)
}

func (sl *SkipList1) Delete(key []byte) (*data.LogRecordPos, bool) {
	sl.lock.Lock()
	element := sl.skl.Remove(key)
	sl.lock.Unlock()
	if element == nil {
		return nil, false
	}
	return element.Value.(*data.LogRecordPos), true
}
func (sl *SkipList1) Iterator(reverse bool) Iterator {
	if sl.skl == nil {
		return nil
	}
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	return newSkipList1Iterator(sl.skl, reverse)
}
func (sl *SkipList1) Close() error {
	sl.skl = sl.skl.Init()
	return nil
}

var _ Indexer = (*SkipList1)(nil)

type SkipList1Iterator struct {
	curIndex int     // 当前遍历的下标位置
	reverse  bool    // 是否是反向遍历
	values   []*Item // key+位置索引信息
}

func newSkipList1Iterator(skl *skiplist.SkipList, reverse bool) *SkipList1Iterator {
	//values := make([]*Item, skl.Len())
	values := make([]*Item, skl.Len())
	var idx int
	if !reverse {
		idx = 0
		//skl.Front()
		element := skl.Front()
		for element != nil {
			values[idx] = &Item{
				key: element.Key().([]byte),
				pos: element.Value.(*data.LogRecordPos),
			}
			idx++
			element = element.Next()
		}
	} else {
		idx = len(values) - 1
		element := skl.Back()
		for element != nil {
			values[idx] = &Item{
				key: element.Key().([]byte),
				pos: element.Value.(*data.LogRecordPos),
			}
			idx--
			element = element.Prev()
		}
	}

	return &SkipList1Iterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (ski *SkipList1Iterator) Rewind() {
	ski.curIndex = 0
}
func (ski *SkipList1Iterator) Seek(key []byte) {
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
func (ski *SkipList1Iterator) Next() {
	ski.curIndex++
}
func (ski *SkipList1Iterator) Valid() bool {
	return ski.curIndex < len(ski.values)
}
func (ski *SkipList1Iterator) Key() []byte {
	return ski.values[ski.curIndex].key
}
func (ski *SkipList1Iterator) Value() *data.LogRecordPos {
	return ski.values[ski.curIndex].pos

}
func (ski *SkipList1Iterator) Close() {
	ski.values = nil
}

var _ Iterator = (*SkipList1Iterator)(nil)
