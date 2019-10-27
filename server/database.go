package server

const (
	cmdDel    = "del"
	cmdExists = "exists"
)

func delCommandFunc(ctx Context) {
	if len(ctx.args) < 2 {
		ctx.Conn.WriteError("ERR wrong number of arguments for '" + string(ctx.args[0]) + "' command")
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
		ctx.Conn.WriteError("ERR wrong number of arguments for '" + string(ctx.args[0]) + "' command")
		return
	}

	err := ctx.db.Exists(ctx.args[1])
	if err != nil {
		ctx.Conn.WriteInt(0)
	} else {
		ctx.Conn.WriteInt(1)
	}
}
