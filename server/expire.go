package server

import (
	"fmt"
	"strconv"
)

const (
	cmdExpire = "expire"
	cmdTTL    = "ttl"
)

func expireCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
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

func ttlCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, string(ctx.args[0])))
		return
	}

	ttl, _ := ctx.db.TTL(ctx.args[1])
	ctx.Conn.WriteInt64(ttl)
}
