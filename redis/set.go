package redis

import (
	bitcask "bitcaskv"
	"encoding/binary"
)

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+8+len(sk.member)+4)
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(len(sk.member)))
	return buf
}

func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, TypeSet)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key,
		meta.version,
		member,
	}

	var ok bool
	if _, err = rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil

}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, TypeSet)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key,
		meta.version,
		member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, TypeSet)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key,
		meta.version,
		member,
	}

	if _, err = rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		return false, nil
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
