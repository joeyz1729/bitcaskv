package redis

import (
	"encoding/binary"
	"time"
)

// ============================= String =============================

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = TypeString
	var index = 1
	var deadline int64 = 0
	if ttl != 0 {
		deadline = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], deadline)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf)
	copy(encValue[index:], value)

	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	dataType := encValue[0]
	if dataType != TypeString {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	deadline, n := binary.Varint(encValue[index:])
	index += n
	if deadline > 0 && deadline <= time.Now().UnixNano() {
		return nil, nil
	}
	return encValue[index:], nil
}
