package storage

type DB interface {
	Close() error

	//string
	Set(key, value []byte) error
	SetNX(key, value []byte) (bool, error)
	Get(key []byte) ([]byte, error)
	GetSet(key, value []byte) ([]byte, error)
	Strlen(key []byte) (int64, error)
	Append(key, value []byte) (int, error)
	Incr(key []byte) (int64, error)
	IncrBy(key []byte, by int64) (int64, error)
	Decr(key []byte) (int64, error)
	DecrBy(key []byte, by int64) (int64, error)
	MSet(keys, values [][]byte) error
	MGet(keys [][]byte) ([][]byte, error)

	//database
	Del(key [][]byte) error
	Exists(key []byte) error
	Rename(key, newkey []byte) error

	//expire
	Expire(key []byte, seconds int) error
}
