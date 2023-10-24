package ds

import (
	"bitcaskv/data"
	"bitcaskv/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSkipList_Put(t *testing.T) {
	skl := NewSkipList()
	key1, value1 := utils.GetTestKey(1), utils.RandomValue(100)
	element := skl.Put(key1, value1)
	assert.Nil(t, element)

	element = skl.Get(key1)
	assert.Equal(t, element.Key(), key1)
	assert.Equal(t, element.Value(), value1)

	key2 := utils.GetTestKey(2)
	value2 := utils.RandomValue(100)
	element2 := skl.Put(key2, value2)
	element = skl.Put(key1, value2)
	assert.Equal(t, element.Value(), value1)
	assert.Nil(t, element2)
	element = skl.Get(key1)
	assert.Equal(t, element.Key(), key1)
	assert.Equal(t, element.Value(), value2)
	assert.Nil(t, element2)
}

func TestSkipList_Remove(t *testing.T) {
	skl := NewSkipList()
	// 放入key为nil，value不为nil的数据
	pos1 := &data.LogRecordPos{Fid: 1, Offset: 100}
	element1 := skl.Put(nil, pos1)
	assert.Nil(t, element1)
	// 删除nil key
	element2 := skl.Remove(nil)
	assert.Nil(t, element2.Key())
	assert.Equal(t, element2.Value().(*data.LogRecordPos), pos1)
	// 删除不存在的key
	element3 := skl.Remove(nil)
	assert.Nil(t, element3)

	// 放入正常的key value
	key1 := utils.GetTestKey(1)
	pos2 := &data.LogRecordPos{Fid: 11, Offset: 222}
	element4 := skl.Put(key1, pos2)
	assert.Nil(t, element4)
	del1 := skl.Remove(key1)
	assert.Equal(t, del1.Key(), key1)
	assert.Equal(t, del1.Value().(*data.LogRecordPos), pos2)
}

func TestSkipList_Exist(t *testing.T) {
	skl := NewSkipList()
	// 放入key为nil，value不为nil的数据
	pos1 := &data.LogRecordPos{Fid: 1, Offset: 100}
	element1 := skl.Put(nil, pos1)
	assert.Nil(t, element1)
	exist1 := skl.Exist(nil)
	assert.True(t, exist1)
	// 删除nil key
	element2 := skl.Remove(nil)
	assert.Nil(t, element2.Key())
	assert.Equal(t, element2.Value().(*data.LogRecordPos), pos1)
	exist2 := skl.Exist(nil)
	assert.False(t, exist2)

	exist3 := skl.Exist(utils.GetTestKey(1))
	assert.False(t, exist3)

}

func TestSkipList_Front_Size(t *testing.T) {
	skl := NewSkipList()
	for i := 0; i < 5; i++ {
		_ = skl.Put(utils.GetTestKey(i+10), utils.RandomValue(100))
	}
	front := skl.Front()
	assert.Equal(t, front.Key(), utils.GetTestKey(10))
	size1 := skl.Size()
	assert.Equal(t, size1, 5)

	for i := 9; i >= 0; i-- {
		key := utils.GetTestKey(i)
		_ = skl.Put(key, utils.RandomValue(100))
		front1 := skl.Front()
		assert.Equal(t, front1.Key(), key)
		size := skl.Size()
		assert.Equal(t, size, 5+10-i)
	}

}

func TestSkipList_Foreach(t *testing.T) {
	skl := NewSkipList()
	for i := 10; i >= 0; i-- {
		_ = skl.Put(utils.GetTestKey(i), utils.RandomValue(100))
	}
	printValue := func(e *Element) bool {
		fmt.Println(string(e.Key()), string(e.Value().([]byte)))
		return true
	}

	skl.Foreach(printValue)

}
