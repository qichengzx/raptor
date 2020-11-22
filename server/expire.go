package server

import (
	"fmt"
	"strconv"
)

const (
	cmdExpire   = "expire"
	cmdPExpire  = "pexpire"
	cmdExpireAt = "expireat"
	cmdTTL      = "ttl"
	cmdPersist  = "persist"
)

func expireCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	seconds, err := strconv.Atoi(string(ctx.args[2]))
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}
	err = ctx.db.Expire(ctx.args[1], seconds)
	if err != nil {
		ctx.Conn.WriteInt(0)
	} else {
		ctx.Conn.WriteInt(1)
	}
}

func pexpireCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	millisecond, err := strconv.Atoi(string(ctx.args[2]))
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	err = ctx.db.Expire(ctx.args[1], millisecond/1000)
	if err != nil {
		ctx.Conn.WriteInt(0)
	} else {
		ctx.Conn.WriteInt(1)
	}
}

func expireatCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	timestamp, err := strconv.ParseInt(string(ctx.args[2]), 10, 64)
	if err != nil {
		ctx.Conn.WriteInt(RespErr)
		return
	}
	err = ctx.db.ExpireAt(ctx.args[1], timestamp)
	if err != nil {
		ctx.Conn.WriteInt(RespErr)
	} else {
		ctx.Conn.WriteInt(RespSucc)
	}
}

func ttlCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	ttl, _ := ctx.db.TTL(ctx.args[1])
	ctx.Conn.WriteInt64(ttl)
}

func persistCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	err := ctx.db.Persist(ctx.args[1])
	if err == nil {
		ctx.Conn.WriteInt(RespSucc)
	} else {
		ctx.Conn.WriteInt(RespErr)
	}
}
