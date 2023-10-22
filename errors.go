package bitcaskv

import "errors"

var (
	ErrKeyIsEmpty = errors.New("the key is empty")

	ErrIndexUpdateFailed = errors.New("failed to update index")

	ErrKeyNotFound      = errors.New("key not exist")
	ErrDataFileNotFound = errors.New("data file not found")

	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")

	ErrExceedmMaxBatchNum = errors.New("exceed the max batch num")
)
