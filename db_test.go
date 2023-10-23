package bitcaskv

import (
	"bitcaskv/utils"
	"bytes"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-list-keys")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 数据库为空
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// 只有一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	// 有多条数据
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(20))
	assert.Nil(t, err)

	keys3 := db.ListKeys()
	assert.Equal(t, 4, len(keys3))
	for _, k := range keys3 {
		assert.NotNil(t, k)
		//t.Logf("[list keys] key: %s\n", k)
	}
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-fold")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		//t.Logf("[fold iterator] key: %s, value: %s\n", key, value)
		// 直到33号退出
		if bytes.HasSuffix(key, []byte("33")) {
			return false
		}
		return true
	})
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-close")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db) // 会调用Close
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)

}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-sync")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-filelock")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 重复打开目录
	_, err = Open(opts)
	assert.NotNil(t, err)
	//t.Log(err)

	// 关闭后打开
	err = db.Close()
	assert.Nil(t, err)
	_, err = Open(opts)
	assert.Nil(t, err)
}

func TestDB_Open2(t *testing.T) {
	opts := DefaultOptions
	opts.MMapAtStartup = true
	opts.DirPath = "/tmp/bitcask-go"
	// 使用mmap打开
	now := time.Now()
	db, err := Open(opts)
	t.Log("open time with mmap", time.Since(now))
	assert.NotNil(t, db)
	assert.Nil(t, err)

	// 关闭重新打开
	err = db.Close()
	assert.Nil(t, err)

	// 使用标准打开
	now = time.Now()
	opts.MMapAtStartup = false
	db2, err := Open(opts)
	t.Log("open time with standard io", time.Since(now))
	assert.NotNil(t, db2)
	assert.Nil(t, err)

	//now = time.Now()
	//for i := 0; i < 1000000; i++ {
	//	key := fmt.Sprintf("test-mmap-%d", i)
	//	value := key
	//	err := db.Put([]byte(key), []byte(value))
	//	assert.Nil(t, err)
	//}
	//t.Log("put data", time.Since(now))

}

func TestDB_Stat(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-stat")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 100; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	for i := 100; i < 1000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 2000; i < 5000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	stat := db.Stat()
	assert.NotNil(t, stat)
	t.Log(stat)
}

//
//func TestDB_Backup(t *testing.T) {
//	opts := DefaultOptions
//	dir, _ := os.MkdirTemp("", "bitcask-go-backup")
//	opts.DirPath = dir
//	db, err := Open(opts)
//	defer destroyDB(db)
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//
//	for i := 1; i < 1000000; i++ {
//		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
//		assert.Nil(t, err)
//	}
//
//	backupDir, _ := os.MkdirTemp("", "bitcask-go-backup-test")
//	err = db.Backup(backupDir)
//	assert.Nil(t, err)
//
//	opts1 := DefaultOptions
//	opts1.DirPath = backupDir
//	db2, err := Open(opts1)
//	defer destroyDB(db2)
//	assert.Nil(t, err)
//	assert.NotNil(t, db2)
//}
