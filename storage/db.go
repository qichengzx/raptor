package storage

type DB interface {
	Close() error

	//string
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Strlen(key []byte) (int64, error)

	//database
	Del(key [][]byte) error
	Exists(key []byte) error
}
