package ds

import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

type handleEle func(e *Element) bool

const (
	maxLevel    int     = 18         // 跳表的最大层数
	probability float64 = 1 / math.E // 层数的概率
)

type Element struct {
	SkipListNode
	key   []byte
	value interface{}
}

type SkipListNode struct {
	next []*Element
}

type SkipList struct {
	node           SkipListNode
	maxLevel       int
	length         int
	randSource     rand.Source
	probability    float64
	probTable      []float64
	prevNodesCache []*SkipListNode
}

func NewSkipList() *SkipList {
	return &SkipList{
		maxLevel:       maxLevel,
		node:           SkipListNode{next: make([]*Element, maxLevel)},
		prevNodesCache: make([]*SkipListNode, maxLevel),
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:    probability,
		probTable:      probabilityTable(probability, maxLevel),
	}
}

func (e *Element) Key() []byte {
	return e.key
}

func (e *Element) Value() interface{} {
	return e.value
}

func (e *Element) SetValue(val interface{}) {
	e.value = val
}

func (e *Element) Next() *Element {
	return e.next[0]
}

func (t *SkipList) Front() *Element {
	return t.node.next[0]
}

func (t *SkipList) Put(key []byte, value interface{}) *Element {
	var element *Element
	prev := t.backNodes(key)
	if element = prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		oldElement := &Element{
			key:   key,
			value: element.value,
		}
		element.value = value
		return oldElement
	}
	element = &Element{
		SkipListNode: SkipListNode{
			next: make([]*Element, t.randomLevel()),
		},
		key:   key,
		value: value,
	}
	for i := range element.next {
		element.next[i] = prev[i].next[i]
		prev[i].next[i] = element
	}
	t.length++
	return nil
}

func (t *SkipList) Get(key []byte) *Element {
	var prev = &t.node
	var next *Element
	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.SkipListNode
			next = next.next[i]
		}
	}

	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}
	return nil
}

func (t *SkipList) Remove(key []byte) *Element {
	prev := t.backNodes(key)
	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		for k, v := range element.next {
			prev[k].next[k] = v
		}

		t.length--
		return element
	}
	return nil
}

func (t *SkipList) Exist(key []byte) bool {
	return t.Get(key) != nil
}

func (t *SkipList) Foreach(fc handleEle) {
	for p := t.Front(); p != nil; p = p.Next() {
		if ok := fc(p); !ok {
			break
		}
	}
}

func (t *SkipList) backNodes(key []byte) []*SkipListNode {
	var prev = &t.node
	var next *Element
	prevs := t.prevNodesCache
	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && bytes.Compare(key, next.Key()) > 0 {
			prev = &next.SkipListNode
			next = next.next[i]
		}
		prevs[i] = prev
	}
	return prevs
}

func (t *SkipList) Size() int {
	return t.length
}

func (t *SkipList) randomLevel() (level int) {
	r := float64(t.randSource.Int63()) / (1 << 63)
	level = 1
	for level < t.maxLevel && r < t.probTable[level] {
		level++
	}
	return
}

func probabilityTable(probability float64, maxLevel int) (table []float64) {
	table = make([]float64, maxLevel)
	for i := 1; i < maxLevel; i++ {
		table[i] = table[i-1] * probability
	}
	return table
}

func (t *SkipList) Close() error {
	t.length = 0
	t.node = SkipListNode{next: make([]*Element, maxLevel)}
	t.prevNodesCache = make([]*SkipListNode, maxLevel)
	return nil
}
