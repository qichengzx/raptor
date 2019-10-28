package server

const (
	RespOK   = "OK"
	RespPong = "PONG"

	ErrWrongArgs = "ERR wrong number of arguments for '%s' command"
	ErrPassword  = "ERR invalid password"
	ErrNoAuth    = "NOAUTH Authentication required"
)
