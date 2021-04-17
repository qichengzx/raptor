package badger

import (
	"errors"
	"github.com/dgraph-io/badger"
	"github.com/qichengzx/raptor/config"
	"time"
)

type BadgerDB struct {
	storage *badger.DB
}

func Open(conf *config.Config) (*BadgerDB, error) {
	opts := badger.DefaultOptions(conf.Raptor.Directory)
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

func (db *BadgerDB) Del(key [][]byte) error {
	return db.storage.Update(func(txn *badger.Txn) error {
		for _, k := range key {
			txn.Delete(k)
		}
		return nil
	})
}

func (db *BadgerDB) Rename(key, newkey []byte, nx bool) error {
	return db.storage.Update(func(txn *badger.Txn) error {
		data, err := db.Get(key)
		if err == badger.ErrKeyNotFound {
			return errors.New("ERR no such key")
		}

		if nx {
			data, err := db.Get(newkey)
			if err == nil && data != nil {
				return errors.New("ERR newkey is exist")
			}
		}

		txn.Delete(key)
		return txn.Set(newkey, data)
	})
}

type ScannerOptions struct {
	Offset      string
	Count       int64
	Prefix      []byte
	FetchValues bool
	Handler     func(k, v []byte)
}

func (db *BadgerDB) Scan(scanOpts ScannerOptions) error {
	err := db.storage.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = scanOpts.FetchValues

		it := txn.NewIterator(opts)
		defer it.Close()

		start := func(it *badger.Iterator) {
			if scanOpts.Offset == "" {
				it.Rewind()
			} else {
				it.Seek([]byte(scanOpts.Offset))
				if it.Valid() {
					it.Next()
				}
			}
		}

		var cnt int64 = 0
		for start(it); it.Valid(); it.Next() {
			if scanOpts.Prefix != nil && !it.ValidForPrefix(scanOpts.Prefix) {
				continue
			}

			var k, v []byte

			item := it.Item()
			k = item.KeyCopy(nil)

			if scanOpts.FetchValues {
				v, _ = item.ValueCopy(nil)
			}

			if scanOpts.Handler != nil {
				scanOpts.Handler(k, v)
			}

			cnt++
			if scanOpts.Count != 0 && cnt >= scanOpts.Count {
				break
			}
		}

		return nil
	})

	return err
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

func (db *BadgerDB) Sync() {
	db.storage.Sync()
}

func (db *BadgerDB) ClearPrefix(prefix []byte) error {
	return db.storage.DropPrefix(prefix)
}
