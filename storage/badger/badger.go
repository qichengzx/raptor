package badger

import (
	"errors"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/qichengzx/raptor/config"
	"strconv"
	"time"
)

type BadgerDB struct {
	storage *badger.DB
}

func Open(conf *config.Config) (*BadgerDB, error) {
	opts := badger.DefaultOptions(conf.Raptor.Directory)
	opts = opts.WithTableLoadingMode(options.MemoryMap).
		WithNumMemtables(2).
		WithValueThreshold(1)

	bdb, err := badger.Open(opts)
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

func (db *BadgerDB) Set(key, value []byte, ttl int) error {
	return db.storage.Update(func(txn *badger.Txn) (err error) {
		e := badger.NewEntry(key, value)
		if ttl > 1 {
			e.WithTTL(time.Duration(ttl) * time.Second)
		}

		return txn.SetEntry(e)
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

func (db *BadgerDB) GetSet(key, value []byte) ([]byte, error) {
	var data []byte
	err := db.storage.Update(func(txn *badger.Txn) error {
		v, err := db.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}

		data = v
		return txn.Set(key, value)
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

func (db *BadgerDB) Append(key, value []byte) (int, error) {
	var length = 0
	err := db.storage.Update(func(txn *badger.Txn) error {
		val, err := db.Get(key)
		if err == nil || err == badger.ErrKeyNotFound {
			val = append(val, value...)

			err := txn.Set(key, val)
			if err != nil {
				return err
			}
			length = len(string(val))
			return nil
		}
		return nil
	})

	if err == nil {
		return length, nil
	}

	return 0, err
}

func (db *BadgerDB) IncrBy(key []byte, by int64) (int64, error) {
	var v int64 = 0
	err := db.storage.Update(func(txn *badger.Txn) error {
		val, err := db.Get(key)
		if err != nil {
			val = []byte("0")
		}

		valInt, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return errors.New("ERR value is not an integer or out of range")
		}
		valInt += by

		valStr := strconv.FormatInt(valInt, 10)
		err = txn.Set(key, []byte(valStr))
		if err != nil {
			return err
		}

		v = valInt
		return nil
	})

	return v, err
}

func (db *BadgerDB) DecrBy(key []byte, by int64) (int64, error) {
	var v int64 = 0
	err := db.storage.Update(func(txn *badger.Txn) error {
		val, err := db.Get(key)
		if err != nil {
			val = []byte("0")
		}

		valInt, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return errors.New("ERR value is not an integer or out of range")
		}
		valInt -= by

		valStr := strconv.FormatInt(valInt, 10)
		err = txn.Set(key, []byte(valStr))
		if err != nil {
			return err
		}

		v = valInt
		return nil
	})

	return v, err
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

func (db *BadgerDB) MSetNX(keys, values [][]byte) error {
	err := db.storage.Update(func(txn *badger.Txn) error {
		writer := db.storage.NewWriteBatch()
		for i, key := range keys {
			v, err := txn.Get(key)
			if err == nil && v != nil {
				writer.Cancel()
				return errors.New("Key exists")
			}

			err = writer.Set(key, values[i])
			if err != nil {
				writer.Cancel()
				return err
			}
		}

		return writer.Flush()
	})

	return err
}

func (db *BadgerDB) MGet(keys [][]byte) ([][]byte, error) {
	var values [][]byte
	err := db.storage.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				values = append(values, nil)
				continue
			}

			data, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}

			values = append(values, data)
		}

		return nil
	})

	return values, err
}

func (db *BadgerDB) Del(key [][]byte) error {
	return db.storage.Update(func(txn *badger.Txn) error {
		for _, k := range key {
			txn.Delete(k)
		}
		return nil
	})
}

func (db *BadgerDB) Rename(key, newkey []byte) error {
	return db.storage.Update(func(txn *badger.Txn) error {
		data, err := db.Get(key)
		if err == badger.ErrKeyNotFound {
			return errors.New("ERR no such key")
		}

		txn.Delete(key)
		return txn.Set(newkey, data)
	})
}

func (db *BadgerDB) RenameNX(key, newkey []byte) error {
	return db.storage.Update(func(txn *badger.Txn) error {
		data, err := db.Get(key)
		if err != nil {
			return errors.New("ERR no such key")
		}

		data2, err := db.Get(newkey)
		if err == nil && data2 != nil {
			return errors.New("ERR newkey is exist")
		}

		txn.Delete(key)
		return txn.Set(newkey, data)
	})
}

func (db *BadgerDB) FlushDB() error {
	return db.storage.DropAll()
}

func (db *BadgerDB) Expire(key []byte, seconds int) error {
	return db.storage.Update(func(txn *badger.Txn) (err error) {
		data, err := db.Get(key)
		if err != nil {
			return err
		}

		e := badger.NewEntry(key, data).WithTTL(time.Duration(seconds) * time.Second)
		return txn.SetEntry(e)
	})
}

func (db *BadgerDB) ExpireAt(key []byte, timestamp int64) error {
	return db.storage.Update(func(txn *badger.Txn) (err error) {
		data, err := db.Get(key)
		if err != nil {
			return err
		}

		ttl := timestamp - time.Now().Unix()
		e := badger.NewEntry(key, data).WithTTL(time.Duration(ttl) * time.Second)
		return txn.SetEntry(e)
	})
}

func (db *BadgerDB) TTL(key []byte) (int64, error) {
	var ttl int64
	err := db.storage.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			ttl = -2
			return err
		}

		ttl = int64(item.ExpiresAt())
		//if not set expire,will return 0,convert to -1
		if ttl == 0 {
			ttl = -1
		} else {
			ttl -= time.Now().Unix()
		}

		return nil
	})

	return ttl, err
}

func (db *BadgerDB) Persist(key []byte) error {
	return db.storage.Update(func(txn *badger.Txn) (err error) {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return err
		}

		if item.ExpiresAt() == 0 {
			return errors.New("")
		}

		data, _ := item.ValueCopy(nil)
		e := badger.NewEntry(key, data)
		return txn.SetEntry(e)
	})
}
