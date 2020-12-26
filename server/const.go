package server

const (
	RespOK   = "OK"
	RespPong = "PONG"
	RespSucc = 1
	RespErr  = 0
	RespSync = "Background saving started"

	ErrTypeNone    = "none"
	ErrKeyNotExist = "Key not found"

	ErrNoAuth     = "NOAUTH Authentication required"
	ErrWrongArgs  = "ERR wrong number of arguments for '%s' command"
	ErrWrongArgsN = "wrong number of arguments (given %d, expected %d)"
	ErrPassword   = "ERR invalid password"
	ErrValue      = "ERR value is not an integer or out of range"
	ErrNoKey      = "ERR no such key"
	ErrKeyExist   = "ERR key is exist"
	ErrSyntax     = "ERR syntax error"
	ErrEmpty      = "empty list or set"
	ErrHashValue  = "ERR hash value is not an integer"
	ErrExpireTime = "ERR invalid expire time in setex"
	ErrWrongType  = "WRONGTYPE Operation against a key holding the wrong kind of value"
)
