package server

const (
	RespOK   = "OK"
	RespPong = "PONG"

	ErrNoAuth    = "NOAUTH Authentication required"
	ErrWrongArgs = "ERR wrong number of arguments for '%s' command"
	ErrPassword  = "ERR invalid password"
	ErrValue     = "ERR value is not an integer or out of range"
)
