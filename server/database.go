package server

import (
	"bytes"
	"fmt"
	"github.com/qichengzx/raptor/storage"
)

const (
	cmdSelect   = "select"
	cmdDel      = "del"
	cmdExists   = "exists"
	cmdRename   = "rename"
	cmdRenameNX = "renamenx"
	cmdFlushDB  = "flushdb"
	cmdFlushAll = "flushall"
	cmdType     = "type"
)

func selectCommandFunc(ctx Context) {
	ctx.Conn.WriteString(RespOK)
}

func delCommandFunc(ctx Context) {
	if len(ctx.args) < 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	err := ctx.db.Del(ctx.args[1:])
	if err != nil {
		ctx.Conn.WriteInt(0)
	} else {
		ctx.Conn.WriteInt(len(ctx.args[1:]))
	}
}

func existsCommandFunc(ctx Context) {
	if len(ctx.args) < 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var cnt = 0
	var byteObjString = byte(storage.ObjectString)
	for _, key := range ctx.args[1:] {
		var buff bytes.Buffer
		buff.WriteByte(byteObjString)
		buff.Write(key)

		_, err := ctx.db.Get(buff.Bytes())
		if err == nil {
			cnt++
		}
	}

	ctx.Conn.WriteInt(cnt)
}

func renameCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	err := ctx.db.Rename(ctx.args[1], ctx.args[2])
	if err != nil {
		ctx.Conn.WriteError(err.Error())
	} else {
		ctx.Conn.WriteString(RespOK)
	}
}

func renamenxCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	err := ctx.db.RenameNX(ctx.args[1], ctx.args[2])
	if err != nil {
		ctx.Conn.WriteInt(RespErr)
	} else {
		ctx.Conn.WriteInt(RespSucc)
	}
}

func flushdbCommandFunc(ctx Context) {
	err := ctx.db.FlushDB()
	if err != nil {
		//TODO
	}
	ctx.Conn.WriteString(RespOK)
}

func flushallCommandFunc(ctx Context) {
	err := ctx.db.FlushDB()
	if err != nil {
		//TODO
	}
	ctx.Conn.WriteString(RespOK)
}

func typeCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var buff bytes.Buffer
	for i := 0; i < len(storage.TypeName); i++ {
		buff.Reset()
		buff.WriteByte(byte(i))
		buff.Write(ctx.args[1])

		_, ok := ctx.db.Get(buff.Bytes())
		if ok != nil {
			ctx.Conn.WriteString("none")
			break
		} else {
			ctx.Conn.WriteString(storage.TypeName[storage.ObjectType(i)])
			break
		}
	}
}
