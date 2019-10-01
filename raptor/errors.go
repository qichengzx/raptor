package raptor

import "errors"

var (
	ErrCommand  = errors.New("ERR command error")
	ErrParams   = errors.New("ERR command params error")
	ErrKeyEmpty = errors.New("ERR key cannot be empty")
)
