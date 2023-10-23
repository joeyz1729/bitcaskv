package index

import (
	"bitcaskv/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const (
	bptreeIndexFileName = "bptree-index"
)

var indexBucketName = []byte("bitcask-index")

// BPlusTree 封装B+树作为内存存储
type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	// 创建对应的bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{
		tree: bptree,
	}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

var _ Indexer = (*BPlusTree)(nil)

// BPlusTree 索引迭代器
type bptreeIterator struct {
	tx       *bbolt.Tx
	cursor   *bbolt.Cursor
	reverse  bool
	curKey   []byte
	curValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	// 初始化后调用，获取curKey，curValue
	bpi.Rewind()
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Last()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.First()
	}

}

func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.curKey, bpi.curValue = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Prev()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.curKey) != 0
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.curKey
}

func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.curValue)
}

func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}
