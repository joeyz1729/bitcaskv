package redis

import (
	bitcask "bitcaskv"
	"errors"
	"time"
)

type redisDataType = byte

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

const (
	TypeString redisDataType = iota + 1
	TypeHash
	TypeList
	TypeSet
	TypeZSet
)

// RedisDataStructure Redis数据结构存储
type RedisDataStructure struct {
	db *bitcask.DB
}

// NewRedisDataStructure 初始化 Redis数据结构服务
func NewRedisDataStructure(options bitcask.Options) (*RedisDataStructure, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

// findMetadata 首先根据 key 和 data type 获取元数据
func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if err == bitcask.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)

		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}

		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == TypeList {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}
