package storage

type DB interface {
	Close() error

	//string
	Set(key, value []byte) error
	SetNX(key, value []byte) (bool, error)
	SetEX(key, value []byte, seconds int) error
	Get(key []byte) ([]byte, error)
	GetSet(key, value []byte) ([]byte, error)
	Strlen(key []byte) (int64, error)
	Append(key, value []byte) (int, error)
	IncrBy(key []byte, by int64) (int64, error)
	DecrBy(key []byte, by int64) (int64, error)
	MSet(keys, values [][]byte) error
	MGet(keys [][]byte) ([][]byte, error)

	//database
	Del(key [][]byte) error
	Exists(key []byte) error
	Rename(key, newkey []byte) error
	RenameNX(key, newkey []byte) error
	FlushDB() error

	//expire
	Expire(key []byte, seconds int) error
	ExpireAt(key []byte, timestamp int64) error
	TTL(key []byte) (int64, error)
	Persist(key []byte) error
}
