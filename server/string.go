package server

import (
	"fmt"
	"strconv"
)

const (
	cmdSet    = "set"
	cmdSetNX  = "setnx"
	cmdGet    = "get"
	cmdStrlen = "strlen"
	cmdIncr   = "incr"
	cmdIncrBy = "incrby"
	cmdDecr   = "decr"
	cmdDecrBy = "decrby"
	cmdMSet   = "mset"
)

func setCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}
	err := ctx.db.Set(ctx.args[1], ctx.args[2])
	if err != nil {
		ctx.Conn.WriteNull()
	} else {
		ctx.Conn.WriteString(RespOK)
	}
}

func setnxCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}
	ok, err := ctx.db.SetNX(ctx.args[1], ctx.args[2])
	if ok && err == nil {
		ctx.Conn.WriteInt(RespSucc)
	} else {
		ctx.Conn.WriteInt(RespErr)
	}
}

func getCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}

	val, ok := ctx.db.Get(ctx.args[1])
	if ok != nil {
		ctx.Conn.WriteNull()
	} else {
		ctx.Conn.WriteBulk(val)
	}
}

func strlenCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}

	val, ok := ctx.db.Get(ctx.args[1])
	if ok != nil {
		ctx.Conn.WriteInt(0)
	} else {
		ctx.Conn.WriteInt(len(string(val)))
	}
}

func incrCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}
	val, err := ctx.db.Incr(ctx.args[1])
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(val)
}

func incrByCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}

	by, err := strconv.ParseInt(string(ctx.args[2]), 10, 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}

	val, err := ctx.db.IncrBy(ctx.args[1], by)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(val)
}

func decrCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}
	val, err := ctx.db.Decr(ctx.args[1])
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(val)
}

func decrByCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}

	by, err := strconv.ParseInt(string(ctx.args[2]), 10, 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}

	val, err := ctx.db.DecrBy(ctx.args[1], by)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(val)
}

func msetCommandFunc(ctx Context) {
	if len(ctx.args) < 2 || len(ctx.args)&1 != 1 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}

	var keys, values [][]byte
	var length = len(ctx.args[1:])
	for i := 0; i < length; i += 2 {
		keys = append(keys, ctx.args[1:][i])
		values = append(values, ctx.args[1:][i+1])
	}

	err := ctx.db.MSet(keys, values)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}

	ctx.Conn.WriteString(RespOK)
}
