package badger

import (
	"github.com/dgraph-io/badger"
	"github.com/qichengzx/raptor/config"
)

type BadgerDB struct {
	storage *badger.DB
}

func Open(conf *config.Config) (*BadgerDB, error) {
	bdb, err := badger.Open(badger.DefaultOptions(conf.Raptor.Directory))
	if err != nil {
		return nil, err
	}

	db := new(BadgerDB)
	db.storage = bdb
	go (func() {
		for db.storage.RunValueLogGC(0.5) == nil {
			// cleaning ...
		}
	})()

	return db, nil
}

// Close close the db
func (db *BadgerDB) Close() error {
	return db.storage.Close()
}

func (db *BadgerDB) Set(key, value []byte) error {
	return db.storage.Update(func(txn *badger.Txn) (err error) {
		return txn.Set(key, value)
	})
}

func (db *BadgerDB) Get(key []byte) ([]byte, error) {
	var data []byte
	err := db.storage.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		data, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})

	return data, err
}

func (db *BadgerDB) Strlen(key []byte) (int64, error) {
	var length int64
	err := db.storage.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		length = item.ValueSize()
		return nil
	})

	return length, err
}

func (db *BadgerDB) Del(key [][]byte) error {
	return db.storage.Update(func(txn *badger.Txn) error {
		for _, k := range key {
			txn.Delete(k)
		}
		return nil
	})
}

func (db *BadgerDB) Exists(key []byte) error {
	return db.storage.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			return err
		}

		return nil
	})
}
