package storage

import (
	log "github.com/sirupsen/logrus"

	"github.com/syndtr/goleveldb/leveldb"
)

type levelDb struct {
	dbPath string
	conn   *leveldb.DB
}

func InitLevelDb(dbPath string) (*levelDb, error) {
	log.Warn("db.InitLevelDb call", "dbPath", dbPath)
	db := new(levelDb)
	db.dbPath = dbPath
	var err error
	db.conn, err = leveldb.OpenFile(dbPath, nil)
	log.Warn("db.InitLevelDb File Opened", "db.conn", db.conn, "err", err)
	return db, err
}

func (db *levelDb) Write(key *[]byte, value *[]byte) error {
	log.Warn("db.Write call", "key", key, "value", value)
	return db.conn.Put(*key, *value, nil)
}

func (db *levelDb) Read(key *[]byte) (*[]byte, error) {
	log.Warn("db.Read call", "key", key)
	value, err := db.conn.Get(*key, nil)
	if err == nil {
		log.Warn("db.Read return no error", "value", value)
		return &value, err
	} else {
		log.Warn("db.Read return error", "err", err)
		return nil, err
	}
}

func (db *levelDb) ReadAll(f func(key, value *[]byte)) error {
	log.Warn("db.ReadAll call", "f", f)
	iter := db.conn.NewIterator(nil, nil)
	for iter.Next() {
		key, value := iter.Key(), iter.Value()
		f(&key, &value)
	}
	iter.Release()
	return iter.Error()
}

func (db *levelDb) Delete(key *[]byte) error {
	log.Warn("db.Delete call", "key", key)
	return db.conn.Delete(*key, nil)
}

func (db *levelDb) Close() error {
	log.Warn("db.Close call", "key", key)
	return db.conn.Close()
}
