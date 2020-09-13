package storage

type DB interface {
	Close() error

	//string
	Set(key, value []byte, ttl int) error
	Get(key []byte) ([]byte, error)
	GetSet(key, value []byte) ([]byte, error)
	Append(key, value []byte) (int, error)
	IncrBy(key []byte, by int64) (int64, error)
	MSet(keys, values [][]byte) error
	MSetNX(keys, values [][]byte) error

	//database
	Del(key [][]byte) error
	Rename(key, newkey []byte) error
	RenameNX(key, newkey []byte) error
	FlushDB() error

	//expire
	Expire(key []byte, seconds int) error
	ExpireAt(key []byte, timestamp int64) error
	TTL(key []byte) (int64, error)
	Persist(key []byte) error
}
