package index

import (
	"bitcaskv/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSkipList_Put(t *testing.T) {
	skl := NewSkipList()

	res1 := skl.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := skl.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	res3 := skl.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 12})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))
}

func TestSkipList_Get(t *testing.T) {
	skl := NewSkipList()

	res1 := skl.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	pos1 := skl.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := skl.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := skl.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))

	pos2 := skl.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestSkipList_Delete(t *testing.T) {
	skl := NewSkipList()
	res1 := skl.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	res2, ok1 := skl.Delete(nil)
	assert.True(t, ok1)
	assert.Equal(t, res2.Fid, uint32(1))
	assert.Equal(t, res2.Offset, int64(100))

	res3 := skl.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)
	res4, ok2 := skl.Delete([]byte("aaa"))
	assert.True(t, ok2)
	assert.Equal(t, res4.Fid, uint32(22))
	assert.Equal(t, res4.Offset, int64(33))
}

func TestSkipList_Iterator(t *testing.T) {
	skl := NewSkipList()
	// 1.SkipList 为空的情况
	iter1 := skl.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	//	2.SkipList 有数据的情况
	skl.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := skl.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	//// 3.有多条数据
	skl.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	skl.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	skl.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := skl.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := skl.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 4.测试 seek
	iter5 := skl.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 5.反向遍历的 seek
	iter6 := skl.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
