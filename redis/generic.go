package redis

import "errors"

var (
	ErrNullValue = errors.New("value is null")
)

func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, ErrNullValue
	}
	return encValue[0], nil
}
