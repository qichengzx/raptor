package server

import "fmt"

const (
	cmdDel     = "del"
	cmdExists  = "exists"
	cmdRename  = "rename"
	cmdFlushDB = "flushdb"
)

func delCommandFunc(ctx Context) {
	if len(ctx.args) < 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}

	err := ctx.db.Del(ctx.args[1:])
	if err != nil {
		ctx.Conn.WriteInt(0)
	} else {
		ctx.Conn.WriteInt(1)
	}
}

func existsCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}

	err := ctx.db.Exists(ctx.args[1])
	if err != nil {
		ctx.Conn.WriteInt(0)
	} else {
		ctx.Conn.WriteInt(1)
	}
}

func renameCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}

	err := ctx.db.Rename(ctx.args[1], ctx.args[2])
	if err != nil {
		ctx.Conn.WriteError(err.Error())
	} else {
		ctx.Conn.WriteString(RespOK)
	}
}

func flushdbCommandFunc(ctx Context) {
	err := ctx.db.FlushDB()
	if err != nil {
		//TODO
	}
	ctx.Conn.WriteString(RespOK)
}
