package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	cmdHSet    = "hset"
	cmdHGet    = "hget"
	cmdHExists = "hexists"
	cmdHDel    = "hdel"
	cmdHLen    = "hlen"
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

	fieldBuff := bytes.NewBuffer([]byte{})
	fieldBuff.Write(typeHash)
	fieldBuff.Write(keySize)
	fieldBuff.Write(key)
	fieldBuff.Write(ctx.args[2])

	v, err := ctx.db.Get(fieldBuff.Bytes())
	if err != nil {
		ctx.Conn.WriteNull()
		return
	}

	ctx.Conn.WriteBulk(v)
}

func hexistsCommandFunc(ctx Context) {
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

	fieldBuff := bytes.NewBuffer([]byte{})
	fieldBuff.Write(typeHash)
	fieldBuff.Write(keySize)
	fieldBuff.Write(key)
	fieldBuff.Write(ctx.args[2])

	_, err = ctx.db.Get(fieldBuff.Bytes())
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt(1)
}

func hdelCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	metaValue, err := typeHashGetMeta(ctx, key)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}
	var keyLen = uint32(len(key))
	var keySize = make([]byte, typeHashKeySize)

	binary.BigEndian.PutUint32(keySize, keyLen)

	var fieldToDel [][]byte
	for _, field := range ctx.args[2:] {
		fieldBuff := bytes.NewBuffer([]byte{})
		fieldBuff.Write(typeHash)
		fieldBuff.Write(keySize)
		fieldBuff.Write(key)
		fieldBuff.Write(field)

		_, err = ctx.db.Get(fieldBuff.Bytes())
		if err == nil {
			fieldToDel = append(fieldToDel, fieldBuff.Bytes())
		}
	}

	var hashSize uint32 = 0
	if metaValue != nil {
		hashSize = binary.BigEndian.Uint32(metaValue[1:5])
	}
	var lenToDel = len(fieldToDel)
	if lenToDel > 0 {
		hashSize -= uint32(lenToDel)
		if hashSize == 0 {
			fieldToDel = append(fieldToDel, key)
		}

		ctx.db.Del(fieldToDel)
		if hashSize == 0 {
			ctx.Conn.WriteInt(lenToDel)
			return
		}
	}

	err = typeHashSetMeta(ctx, key, hashSize)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt(lenToDel)
}

func hlenCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	metaValue, err := typeHashGetMeta(ctx, ctx.args[1])
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}

	if metaValue == nil {
		ctx.Conn.WriteInt(0)
		return
	}

	var hashSize uint32 = 0
	if metaValue != nil {
		hashSize = binary.BigEndian.Uint32(metaValue[1:5])
	}

	ctx.Conn.WriteInt(int(hashSize))
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
