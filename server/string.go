package server

const (
	cmdSet = "set"
	cmdGet = "get"
)

func setCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError("ERR wrong number of arguments for '" + string(ctx.args[0]) + "' command")
		return
	}
	err := ctx.db.Set(ctx.args[1], ctx.args[2])
	if err != nil {
		ctx.Conn.WriteError("")
	} else {
		ctx.Conn.WriteString(RespOK)
	}
}

func getCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError("ERR wrong number of arguments for '" + string(ctx.args[0]) + "' command")
		return
	}

	val, ok := ctx.db.Get(ctx.args[1])
	if ok != nil {
		ctx.Conn.WriteNull()
	} else {
		ctx.Conn.WriteBulk(val)
	}
}
