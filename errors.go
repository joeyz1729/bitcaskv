package bitcaskv

import "errors"

var (
	ErrKeyIsEmpty = errors.New("the key is empty")

	ErrIndexUpdateFailed = errors.New("failed to update index")

	ErrKeyNotFound      = errors.New("key not exist")
	ErrDataFileNotFound = errors.New("data file not found")

	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")

	ErrExceedMaxBatchNum = errors.New("exceed the max batch num")

	ErrMergeIsProgress   = errors.New("merge is in progress, try again later")
	ErrInvalidMergeRatio = errors.New("invalid merge ratio, must between 0 and 1")

	ErrDatabaseIsUsing = errors.New("the database directory is used by another process")

	ErrMergeRatioUnreached   = errors.New("the merge ratio do not reach the option")
	ErrNoEnoughSpaceForMerge = errors.New("no enough disk space for merge")
)
