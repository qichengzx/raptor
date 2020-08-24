package server

import (
	"fmt"
	"strconv"
)

const (
	cmdSet    = "set"
	cmdSetNX  = "setnx"
	cmdSetEX  = "setex"
	cmdGet    = "get"
	cmdGetSet = "getset"
	cmdStrlen = "strlen"
	cmdAppend = "append"
	cmdIncr   = "incr"
	cmdIncrBy = "incrby"
	cmdDecr   = "decr"
	cmdDecrBy = "decrby"
	cmdMSet   = "mset"
	cmdMSetNX = "msetnx"
	cmdMGet   = "mget"
)

func setCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}
	err := ctx.db.Set(ctx.args[1], ctx.args[2], 0)
	if err != nil {
		ctx.Conn.WriteNull()
	} else {
		ctx.Conn.WriteString(RespOK)
	}
}

func setnxCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}
	ok, err := ctx.db.SetNX(ctx.args[1], ctx.args[2])
	if ok && err == nil {
		ctx.Conn.WriteInt(RespSucc)
	} else {
		ctx.Conn.WriteInt(RespErr)
	}
}

func setexCommandFunc(ctx Context) {
	if len(ctx.args) != 4 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}
	seconds, err := strconv.Atoi(string(ctx.args[2]))
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}

	if seconds < 1 {
		ctx.Conn.WriteError(ErrExpireTime)
		return
	}

	err = ctx.db.Set(ctx.args[1], ctx.args[2], seconds)
	if err == nil {
		ctx.Conn.WriteString(RespOK)
	} else {
		ctx.Conn.WriteNull()
	}
}

func getCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	val, ok := ctx.db.Get(ctx.args[1])
	if ok != nil {
		ctx.Conn.WriteNull()
	} else {
		ctx.Conn.WriteBulk(val)
	}
}

func getsetCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}
	val, err := ctx.db.GetSet(ctx.args[1], ctx.args[2])
	if err == nil && val == nil {
		ctx.Conn.WriteNull()
	} else if val != nil {
		ctx.Conn.WriteBulk(val)
	} else {
		ctx.Conn.WriteNull()
	}
}

func strlenCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	val, ok := ctx.db.Get(ctx.args[1])
	if ok != nil {
		ctx.Conn.WriteInt(0)
	} else {
		ctx.Conn.WriteInt(len(string(val)))
	}
}

func appendCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}
	length, err := ctx.db.Append(ctx.args[1], ctx.args[2])
	if err == nil {
		ctx.Conn.WriteInt(length)
	} else {
		ctx.Conn.WriteInt(0)
	}
}

func incrCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}
	val, err := ctx.db.IncrBy(ctx.args[1], 1)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(val)
}

func incrByCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
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
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}
	val, err := ctx.db.DecrBy(ctx.args[1], 1)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(val)
}

func decrByCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
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
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
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

func msetnxCommandFunc(ctx Context) {
	if len(ctx.args) < 2 || len(ctx.args)&1 != 1 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var keys, values [][]byte
	var length = len(ctx.args[1:])
	for i := 0; i < length; i += 2 {
		keys = append(keys, ctx.args[1:][i])
		values = append(values, ctx.args[1:][i+1])
	}

	err := ctx.db.MSetNX(keys, values)
	if err != nil {
		ctx.Conn.WriteInt(0)
	} else {
		ctx.Conn.WriteInt(1)
	}
}

func mgetCommandFunc(ctx Context) {
	if len(ctx.args) < 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var keys = ctx.args[1:]
	values, err := ctx.db.MGet(keys)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}

	ctx.Conn.WriteArray(len(values))
	for _, v := range values {
		if v == nil {
			ctx.Conn.WriteNull()
		} else {
			ctx.Conn.WriteBulk(v)
		}
	}
}
