package index

import (
	"bitcaskv/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Get([]byte("abc"))
	assert.NotNil(t, pos1)
	assert.Equal(t, int64(pos1.Offset), int64(999))
	assert.Equal(t, uint32(pos1.Fid), uint32(123))
	pos2 := tree.Get([]byte("ccc"))
	assert.Nil(t, pos2)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 333, Offset: 666})
	pos4 := tree.Get([]byte("aac"))
	assert.Equal(t, int64(pos4.Offset), int64(666))
	assert.Equal(t, uint32(pos4.Fid), uint32(333))
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	// 1. 删除不存在
	deleted := tree.Delete([]byte("aaa"))
	assert.Equal(t, deleted, false)

	// 2. 放入数据
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	//tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos1)
	// 3. 删除已有数据
	deleted2 := tree.Delete([]byte("aac"))
	assert.Equal(t, deleted2, true)
	// 4. 获取被删除的数据
	pos2 := tree.Get([]byte("aac"))
	assert.Nil(t, pos2)

}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	size1 := tree.Size()
	assert.Equal(t, size1, 0)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	size2 := tree.Size()
	assert.Equal(t, size2, 1)
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	size3 := tree.Size()
	assert.Equal(t, size3, 2)
	tree.Delete([]byte("abc"))
	size4 := tree.Size()
	assert.Equal(t, size4, 1)
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-iter")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("cac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("bac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("eac"), &data.LogRecordPos{Fid: 123, Offset: 999})

	iter1 := tree.Iterator(true)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		t.Log(string(iter1.Key()))
	}

}
