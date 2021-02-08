package server

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	cmdSet         = "set"
	cmdSetNX       = "setnx"
	cmdSetEX       = "setex"
	cmdPSetEX      = "psetex"
	cmdGet         = "get"
	cmdGetSet      = "getset"
	cmdStrlen      = "strlen"
	cmdAppend      = "append"
	cmdGetRange    = "getrange"
	cmdIncr        = "incr"
	cmdIncrBy      = "incrby"
	cmdDecr        = "decr"
	cmdDecrBy      = "decrby"
	cmdIncrByFloat = "incrbyfloat"
	cmdMSet        = "mset"
	cmdMSetNX      = "msetnx"
	cmdMGet        = "mget"
)

var typeString = []byte("s")

func setCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	err := ctx.db.Set(ctx.args[1], append(typeString, ctx.args[2]...), 0)
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

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	if val != nil {
		ctx.Conn.WriteInt(RespErr)
		return
	}

	err = ctx.db.Set(ctx.args[1], append(typeString, ctx.args[2]...), 0)
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

	err = ctx.db.Set(ctx.args[1], append(typeString, ctx.args[3]...), seconds)
	if err == nil {
		ctx.Conn.WriteString(RespOK)
	} else {
		ctx.Conn.WriteNull()
	}
}

func psetexCommandFunc(ctx Context) {
	if len(ctx.args) != 4 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	millisecond, err := strconv.Atoi(string(ctx.args[2]))
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}
	var seconds = millisecond / 1000
	if seconds < 1 {
		ctx.Conn.WriteError(ErrExpireTime)
		return
	}

	err = ctx.db.Set(ctx.args[1], append(typeString, ctx.args[3]...), seconds)
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

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil {
		if err.Error() == ErrKeyNotExist {
			ctx.Conn.WriteNull()
			return
		}

		ctx.Conn.WriteError(err.Error())
	} else {
		ctx.Conn.WriteBulk(val[1:])
	}
}

func getsetCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
	}

	err = ctx.db.Set(ctx.args[1], append(typeString, ctx.args[2]...), 0)
	if err != nil {
		ctx.Conn.WriteNull()
		return
	}
	if val == nil {
		ctx.Conn.WriteNull()
		return
	}
	ctx.Conn.WriteBulk(val[1:])
}

func strlenCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil {
		if err.Error() == ErrKeyNotExist {
			ctx.Conn.WriteInt(0)
			return
		}

		ctx.Conn.WriteError(err.Error())
	}

	ctx.Conn.WriteInt(len(string(val[1:])))
}

func appendCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}
	if err != nil && val == nil {
		val = typeString
	}

	val = append(val, ctx.args[2]...)
	err = ctx.db.Set(ctx.args[1], val, 0)
	if err == nil {
		ctx.Conn.WriteInt(len(val[1:]))
	} else {
		ctx.Conn.WriteInt(0)
	}
}

func getrangeCommandFunc(ctx Context) {
	if len(ctx.args) != 4 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}
	if val == nil {
		ctx.Conn.WriteString("")
		return
	}

	offset, err := strconv.ParseInt(string(ctx.args[2]), 10, 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}
	end, err := strconv.ParseInt(string(ctx.args[3]), 10, 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}

	length := int64(len(val[1:]))
	if offset > length {
		ctx.Conn.WriteString("")
		return
	}

	if end >= length {
		ctx.Conn.WriteBulk(val[offset+1:])
		return
	}
	ctx.Conn.WriteBulk(val[offset+1 : end+2])
}

func incrCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}
	if err != nil {
		val = []byte("0")
	}

	valInt, err := strconv.ParseInt(string(val[1:]), 10, 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}
	valInt += 1
	valStr := strconv.FormatInt(valInt, 10)

	err = ctx.db.Set(ctx.args[1], append(typeString, []byte(valStr)...), 0)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(valInt)
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

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}
	if err != nil {
		val = []byte("0")
	}

	valInt, err := strconv.ParseInt(string(val[1:]), 10, 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}
	valInt += by
	valStr := strconv.FormatInt(valInt, 10)

	err = ctx.db.Set(ctx.args[1], append(typeString, []byte(valStr)...), 0)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(valInt)
}

func decrCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}
	if err != nil {
		val = []byte("0")
	}

	valInt, err := strconv.ParseInt(string(val[1:]), 10, 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}
	valInt -= 1
	valStr := strconv.FormatInt(valInt, 10)

	err = ctx.db.Set(ctx.args[1], append(typeString, []byte(valStr)...), 0)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(valInt)
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

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}
	if err != nil {
		val = []byte("0")
	}

	valInt, err := strconv.ParseInt(string(val[1:]), 10, 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
	}
	valInt -= by
	valStr := strconv.FormatInt(valInt, 10)

	err = ctx.db.Set(ctx.args[1], append(typeString, []byte(valStr)...), 0)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteInt64(valInt)
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

	val, err := typeStringGetVal(ctx, ctx.args[1])
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}
	if err != nil {
		val = []byte("0")
	}

	valFloat, err := strconv.ParseFloat(string(val[1:]), 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}
	valFloat += by

	valStr := strconv.FormatFloat(valFloat, 'e', -1, 64)
	err = ctx.db.Set(ctx.args[1], append(typeString, []byte(valStr)...), 0)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	ctx.Conn.WriteString(strconv.FormatFloat(valFloat, 'f', 17, 64))
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
		values = append(values, append(typeString, ctx.args[1:][i+1]...))
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
		values = append(values, append(typeString, ctx.args[1:][i+1]...))
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

	for _, key := range ctx.args[1:] {
		data, err := ctx.db.Get(key)
		if err != nil {
			values = append(values, nil)
			continue
		}

		values = append(values, data[1:])
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

func typeStringGetVal(ctx Context, key []byte) ([]byte, error) {
	val, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		return nil, err
	}

	if val != nil && len(val) > 1 {
		if string(val[0]) != string(typeString) {
			return nil, errors.New(ErrWrongType)
		}
	}

	return val, nil
}
