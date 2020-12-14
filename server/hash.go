package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	cmdHSet = "hset"
	cmdHGet = "hget"
)

const (
	typeHashKeySize = 4
)

var (
	typeHash     = []byte("H")
	typeHashSize = uint32(len(typeSet))
)

func hsetCommandFunc(ctx Context) {
	if len(ctx.args) != 4 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	var keyLen = uint32(len(key))
	var keySize = make([]byte, typeHashKeySize)
	var hashSize uint32 = 0

	metaValue, err := typeHashGetMeta(ctx, key)
	if err == nil && metaValue != nil {
		hashSize = binary.BigEndian.Uint32(metaValue[1:5])
	} else {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		}
	}

	binary.BigEndian.PutUint32(keySize, keyLen)
	fieldBuff := bytes.NewBuffer([]byte{})
	fieldBuff.Write(typeHash)
	fieldBuff.Write(keySize)
	fieldBuff.Write(key)
	fieldBuff.Write(ctx.args[2])

	err = ctx.db.Set(fieldBuff.Bytes(), ctx.args[3], 0)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}
	hashSize += 1

	err = typeHashSetMeta(ctx, key, hashSize)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt(1)
}

func hgetCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	_, err := typeHashGetMeta(ctx, key)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	var keyLen = uint32(len(key))
	var keySize = make([]byte, typeHashKeySize)

	binary.BigEndian.PutUint32(keySize, keyLen)
	var field = ctx.args[2]

	fieldBuff := bytes.NewBuffer([]byte{})
	fieldBuff.Write(typeHash)
	fieldBuff.Write(keySize)
	fieldBuff.Write(key)
	fieldBuff.Write(field)

	v, err := ctx.db.Get(fieldBuff.Bytes())
	if err != nil {
		ctx.Conn.WriteNull()
		return
	}
	
	ctx.Conn.WriteBulk(v)
}

func typeHashGetMeta(ctx Context, key []byte) ([]byte, error) {
	metaValue, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		return nil, err
	}

	if metaValue != nil && len(metaValue) > 1 {
		if string(metaValue[0]) != string(typeHash) {
			return nil, errors.New(ErrWrongType)
		}
	}

	return metaValue, nil
}

func typeHashSetMeta(ctx Context, key []byte, size uint32) error {
	var metaValue = make([]byte, typeHashKeySize)
	binary.BigEndian.PutUint32(metaValue, size)

	return ctx.db.Set(key, append(typeHash, metaValue...), 0)
}
