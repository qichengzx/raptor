package server

import "fmt"

const (
	cmdEcho = "echo"
	cmdPing = "ping"
)

func echoCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}

	ctx.Conn.WriteString(string(ctx.args[1]))
}
func pingCommandFunc(ctx Context) {
	ctx.Conn.WriteString(RespPong)
}
