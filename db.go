package bitcaskv

import (
	"bitcaskv/data"
	"bitcaskv/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB 存储引擎实例
type DB struct {
	options Options
	mu      *sync.RWMutex

	activeFile *data.DataFile            // 当前的活跃文件，数据写入到此
	olderFiles map[uint32]*data.DataFile // 已经封装的文件，通过fid定位，只用于读
	index      index.Indexer             // 内存索引

	seqNo uint64 // 事务的序号

	_fileIds []int // 文件id，仅用于open时加载索引

	isMerging       bool // 是否正在 merge
	seqNoFileExists bool // 存储事务序列号的文件是否存在
	isInitial       bool // 是否是第一次初始化此数据目录
	//fileLock        *flock.Flock              // 文件锁保证多进程之间的互斥
	bytesWrite  uint  // 累计写了多少个字节
	reclaimSize int64 // 表示有多少数据是无效的
}

// Open 打开数据库实例
func Open(options Options) (*DB, error) {
	// 配置项校验
	// TODO option设计模式
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	// 如果数据库目录不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(index.IndexerType(options.IndexType)),
	}

	// 加载 merge 目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 从磁盘数据目录中加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从 hint 中查看是否有索引文件
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 加载内存索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	// 返回数据库实例
	return db, nil
}

// Put 将kv键值对写入到数据库中
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 创建record
	record := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// 添加记录，并获得位置信息
	pos, err := db.appendLogRecordWithLock(record)
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

	// 根据位置信息获取log record
	return db.getValueByPosition(pos)

}

// ListKeys 获取数据库中所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有数据，并全部执行指定的操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	iterator := db.index.Iterator(false)
	//defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// getValueByPosition 通过位置信息获取log record
func (db *DB) getValueByPosition(pos *data.LogRecordPos) (value []byte, err error) {

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
	record, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if record.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 判断是否存在
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	// 添加删除的entry
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// appendLogRecordWithLock 添加数据并返回记录的位置信息
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 添加数据并返回记录的位置信息
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}

// loadDataFiles 加载磁盘数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	// 获取所有文件名字为*.data的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 001.data
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 对文件排序，从小到大加载
	sort.Ints(fileIds)
	db._fileIds = fileIds
	// 遍历并打开数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		// 判断是活跃文件还是已经封装的文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

func (db *DB) loadIndexFromDataFiles() error {
	// TODO
	if len(db._fileIds) == 0 {
		// 空数据库，返回
		return nil
	}

	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err != nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		// 更新offset
		if !ok {
			//return ErrIndexUpdateFailed
			panic("failed to update index at startup")
		}
	}

	// 暂存事务数据，每个事务id对应一个记录列表
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	for i, fid := range db._fileIds {
		var fileId = uint32(fid)
		// 如果文件比 hint 文件中标识的id更小，说明已经加载过了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		// 根据 fid 获取data file
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 拿到数据记录后，保存到内存索引当中
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 普通操作，非batch提交
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 如果读取到事务提交的标志
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					// 是batch的数据，但是还没有标志标识已经提交成功
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			offset += size
		}

		// 如果是当前活跃文件，更新文件的offset
		if i == len(db._fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新db的事务id
	db.seqNo = currentSeqNo
	return nil
}

func (db *DB) Close() error {
	if db.activeFile == nil {
		// TODO, 不应该也关闭older吗
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}
