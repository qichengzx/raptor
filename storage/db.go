package storage

type DB interface {
	Close() error

	//string
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Strlen(key []byte) (int64, error)
	Incr(key []byte) (int64, error)
	IncrBy(key []byte, by int64) (int64, error)
	Decr(key []byte) (int64, error)
	DecrBy(key []byte, by int64) (int64, error)
	MSet(keys, values [][]byte) error

	//database
	Del(key [][]byte) error
	Exists(key []byte) error
}
