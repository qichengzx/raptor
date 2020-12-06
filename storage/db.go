package storage

import "github.com/qichengzx/raptor/storage/badger"

type DB interface {
	Close() error

	//string
	Set(key, value []byte, ttl int) error
	Get(key []byte) ([]byte, error)
	MSet(keys, values [][]byte) error
	MSetNX(keys, values [][]byte) error

	//database
	Del(key [][]byte) error
	Rename(key, newkey []byte) error
	RenameNX(key, newkey []byte) error
	Scan(opts badger.ScannerOptions) error
	FlushDB() error

	//expire
	Expire(key []byte, seconds int) error
	ExpireAt(key []byte, timestamp int64) error
	TTL(key []byte) (int64, error)
	Persist(key []byte) error
}

type ObjectType []byte

var (
	ObjectString = "s"
	ObjectList   = "l"
	ObjectHash   = "h"
	ObjectSet    = "S"
	ObjectZset   = "z"
)

var TypeName = map[string]string{
	ObjectString: "string",
	ObjectList:   "list",
	ObjectHash:   "hash",
	ObjectSet:    "set",
	ObjectZset:   "zset",
}
