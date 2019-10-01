package storage

type DB interface {
	Close() error

	//string
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
}
