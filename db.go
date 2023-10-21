package bitcaskv

import (
	"bitcaskv/data"
	"bitcaskv/index"
	"sync"
)

// DB 存储引擎实例
type DB struct {
	activeFile *data.DataFile            // 当前的活跃文件，数据写入到此
	olderFiles map[uint32]*data.DataFile // 已经封装的文件，通过fid定位，只用于读
	index      index.Indexer
	options    Options
	mu         *sync.RWMutex
}

// Put 将kv键值对写入到数据库中
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 创建record
	record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// 添加记录，并获得位置信息
	pos, err := db.appendLogRecord(record)
	if err != nil {
		return err
	}
	// 添加位置信息的内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// TODO
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	// 从内存索引中拿出位置信息
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	// 根据位置信息查找数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	// 根据偏移量从数据文件中读取
	record, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if record.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return record.Value, nil

}

// appendLogRecord 添加数据并返回记录的位置信息
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	// 判断活跃文件是否存在，如果不存在则初始化
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 将log record编码
	encRes, size := data.EncodeLogRecord(logRecord)
	// 判断活跃文件空间是否足够放入
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 持久化并创建新的活跃文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 活跃文件封装并创建新的活跃文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRes); err != nil {
		return nil, err
	}

	// 如果开启安全持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil

}

// setActiveDataFile 设置数据库当前操作的文件
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
