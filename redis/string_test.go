package redis

import (
	bitcask "bitcaskv"
	"bitcaskv/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestRedisDataStructure_Get_Set(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	// 无过期时间
	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	// 有过期时间
	err = rds.Set(utils.GetTestKey(2), time.Millisecond*500, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Len(t, val1, 100)

	time.Sleep(time.Second)
	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Nil(t, val2)
}
