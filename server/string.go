package server

import (
	"bytes"
	"fmt"
	"github.com/qichengzx/raptor/storage"
	"strconv"
)

const (
	cmdSet         = "set"
	cmdSetNX       = "setnx"
	cmdSetEX       = "setex"
	cmdGet         = "get"
	cmdGetSet      = "getset"
	cmdStrlen      = "strlen"
	cmdAppend      = "append"
	cmdIncr        = "incr"
	cmdIncrBy      = "incrby"
	cmdDecr        = "decr"
	cmdDecrBy      = "decrby"
	cmdIncrByFloat = "incrbyfloat"
	cmdMSet        = "mset"
	cmdMSetNX      = "msetnx"
	cmdMGet        = "mget"
)

func setCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}
	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	err := ctx.db.Set(buff.Bytes(), ctx.args[2], 0)
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

	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	val, err := ctx.db.Get(buff.Bytes())
	if err == nil && val != nil {
		ctx.Conn.WriteInt(RespErr)
		return
	}

	if err.Error() != "Key not found" {
		ctx.Conn.WriteInt(RespErr)
		return
	}

	err = ctx.db.Set(buff.Bytes(), ctx.args[2], 0)
	if err != nil {
		ctx.Conn.WriteInt(RespErr)
	} else {
		ctx.Conn.WriteInt(RespSucc)
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

	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	err = ctx.db.Set(buff.Bytes(), ctx.args[2], seconds)
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
	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	val, ok := ctx.db.Get(buff.Bytes())
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

	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	val, err := ctx.db.GetSet(buff.Bytes(), ctx.args[2])
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

	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	val, ok := ctx.db.Get(buff.Bytes())
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
	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	length, err := ctx.db.Append(buff.Bytes(), ctx.args[2])
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
	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	val, err := ctx.db.IncrBy(buff.Bytes(), 1)
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

	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	val, err := ctx.db.IncrBy(buff.Bytes(), by)
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
	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	val, err := ctx.db.IncrBy(buff.Bytes(), -1)
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

	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	val, err := ctx.db.IncrBy(buff.Bytes(), -by)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(val)
}

func incrByFloatCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	by, err := strconv.ParseFloat(string(ctx.args[2]), 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}

	var buff bytes.Buffer
	buff.WriteByte(byte(storage.ObjectString))
	buff.Write(ctx.args[1])

	val, err := ctx.db.IncrByFloat(buff.Bytes(), by)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteString(strconv.FormatFloat(val, 'f', 17, 64))
}

func msetCommandFunc(ctx Context) {
	if len(ctx.args) < 2 || len(ctx.args)&1 != 1 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var keys, values [][]byte
	var length = len(ctx.args[1:])
	var byteObjString = byte(storage.ObjectString)

	for i := 0; i < length; i += 2 {
		var buff bytes.Buffer
		buff.WriteByte(byteObjString)
		buff.Write(ctx.args[1:][i])

		keys = append(keys, buff.Bytes())
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
	var byteObjString = byte(storage.ObjectString)

	for i := 0; i < length; i += 2 {
		var buff bytes.Buffer
		buff.WriteByte(byteObjString)
		buff.Write(ctx.args[1:][i])

		keys = append(keys, buff.Bytes())
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
	var values [][]byte
	var byteObjString = byte(storage.ObjectString)

	for _, key := range ctx.args[1:] {
		var buff bytes.Buffer
		buff.WriteByte(byteObjString)
		buff.Write(key)

		data, err := ctx.db.Get(buff.Bytes())
		if err != nil {
			values = append(values, nil)
			continue
		}

		values = append(values, data)
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
