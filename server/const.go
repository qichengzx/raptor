package server

const (
	RespOK   = "OK"
	RespPong = "PONG"
	RespSucc = 1
	RespErr  = 0

	ErrNoAuth    = "NOAUTH Authentication required"
	ErrWrongArgs = "ERR wrong number of arguments for '%s' command"
	ErrPassword  = "ERR invalid password"
	ErrValue     = "ERR value is not an integer or out of range"
	ErrNoKey     = "ERR no such key"
	ErrKeyExist  = "ERR key is exist"
	ErrSyntax    = "ERR syntax error"
)
