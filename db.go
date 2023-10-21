package bitcaskv

import (
	"bitcaskv/data"
	"bitcaskv/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB 存储引擎实例
type DB struct {
	activeFile *data.DataFile            // 当前的活跃文件，数据写入到此
	olderFiles map[uint32]*data.DataFile // 已经封装的文件，通过fid定位，只用于读
	index      index.Indexer
	options    Options
	mu         *sync.RWMutex

	_fileIds []int // 文件id，仅用于open时加载索引
}

// Open 打开数据库实例
func Open(options Options) (*DB, error) {
	// 配置项校验
	// TODO option设计模式
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	// 如果数据库目录不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsExist(err) {
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
	// 从磁盘数据目录中加载数据文件
	if err := db.loadDataFiles(); err != nil {
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
	record, _, err := dataFile.ReadLogRecord(pos.Offset)
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

	for i, fid := range db._fileIds {
		// 根据 fid 获取data file
		var fileId = uint32(fid)
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
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}
			// 更新offset
			offset += size
		}

		// 如果是当前活跃文件，更新文件的offset
		if i == len(db._fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}