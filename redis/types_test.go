package redis

import (
	bitcask "bitcaskv"
	"bitcaskv/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-del")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	// 删除不存在的key
	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)
	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	typ, err = rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, typ, TypeString)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
	assert.Nil(t, val1)
}
