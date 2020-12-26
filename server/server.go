package server

import "fmt"

const (
	cmdSave   = "save"
	cmdBgSave = "bgsave"
)

func saveCommandFunc(ctx Context) {
	if len(ctx.args) != 1 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	ctx.db.Sync()
	ctx.Conn.WriteString(RespSync)
}

func bgsaveCommandFunc(ctx Context) {
	if len(ctx.args) != 1 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	ctx.db.Sync()
	ctx.Conn.WriteString(RespSync)
}
