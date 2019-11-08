package badger

import (
	"github.com/dgraph-io/badger"
	"github.com/qichengzx/raptor/config"
	"strconv"
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

func (db *BadgerDB) SetNX(key, value []byte) (bool, error) {
	_, err := db.Get(key)
	if err == badger.ErrKeyNotFound {
		err = db.Set(key, value)
		return true, err
	}

	return false, nil
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

func (db *BadgerDB) Incr(key []byte) (int64, error) {
	return db.IncrBy(key, 1)
}

func (db *BadgerDB) IncrBy(key []byte, by int64) (int64, error) {
	val, err := db.Get(key)
	if err != nil {
		val = []byte("0")
	}

	valInt, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, err
	}
	valInt += by

	valStr := strconv.FormatInt(valInt, 10)
	err = db.Set(key, []byte(valStr))
	if err != nil {
		return 0, err
	}

	return valInt, nil
}

func (db *BadgerDB) Decr(key []byte) (int64, error) {
	return db.DecrBy(key, 1)
}

func (db *BadgerDB) DecrBy(key []byte, by int64) (int64, error) {
	val, err := db.Get(key)
	if err != nil {
		val = []byte("0")
	}

	valInt, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, err
	}
	valInt -= by

	valStr := strconv.FormatInt(valInt, 10)
	err = db.Set(key, []byte(valStr))
	if err != nil {
		return 0, err
	}

	return valInt, nil
}

func (db *BadgerDB) MSet(keys, values [][]byte) error {
	var err error
	writer := db.storage.NewWriteBatch()
	for i, key := range keys {
		err = writer.Set(key, values[i])
		if err != nil {
			writer.Cancel()
			return err
		}
	}

	return writer.Flush()
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
