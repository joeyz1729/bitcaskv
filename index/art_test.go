package index

import (
	"bitcaskv/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 2, Offset: 22})

	pos1 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos1)
	pos2 := art.Get([]byte("key-2"))
	assert.NotNil(t, pos2)
	//t.Log(pos1, pos2, pos3)
	pos4 := art.Get([]byte("key-4"))
	assert.Nil(t, pos4)
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1111, Offset: 1111})
	pos5 := art.Get([]byte("key-1"))
	t.Log(pos5)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 2, Offset: 22})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 3, Offset: 32})

	pos1 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos1)
	pos2 := art.Get([]byte("key-2"))
	assert.NotNil(t, pos2)

	deleted1 := art.Delete([]byte("key-4"))
	assert.Equal(t, deleted1, false)
	deleted2 := art.Delete([]byte("key-3"))
	assert.Equal(t, deleted2, true)
	pos4 := art.Get([]byte("key-3"))
	assert.Nil(t, pos4)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	size1 := art.Size()
	assert.Equal(t, size1, 0)
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	size2 := art.Size()
	assert.Equal(t, size2, 1)
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 2, Offset: 22})
	size3 := art.Size()
	assert.Equal(t, size3, 2)
	art.Delete([]byte("key-1"))
	size4 := art.Size()
	assert.Equal(t, size4, 1)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("adse"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bade"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
		t.Log(string(iter.Key()), iter.Value())
	}

	iter = art.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
		t.Log(string(iter.Key()), iter.Value())
	}
}
