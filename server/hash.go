package server

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/qichengzx/raptor/storage/badger"
	"strconv"
)

const (
	cmdHSet    = "hset"
	cmdHSetNX  = "hsetnx"
	cmdHGet    = "hget"
	cmdHExists = "hexists"
	cmdHDel    = "hdel"
	cmdHLen    = "hlen"
	cmdHStrLen = "hstrlen"
	cmdHIncrby = "hincrby"
	cmdHGetall = "hgetall"
	cmdHMSet   = "hmset"
	cmdHMGet   = "hmget"
	cmdHKeys   = "hkeys"
	cmdHVals   = "hvals"
)

const (
	typeHashKeySize = 4
)

var (
	typeHash     = []byte("H")
	typeHashSize = uint32(len(typeHash))
)

func hsetCommandFunc(ctx Context) {
	if len(ctx.args) != 4 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var (
		key             = ctx.args[1]
		hashSize uint32 = 0
		keys     [][]byte
		values   [][]byte
	)

	metaValue, err := typeHashGetMeta(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	if metaValue != nil {
		hashSize = bytesToUint32(metaValue[1:5])
	}

	field := typeHashMarshalField(key, ctx.args[2])
	exists, err := ctx.db.Get(field)
	if exists == nil {
		hashSize += 1
	}
	keys = append(keys, key, field)
	values = append(values, typeHashMetaVal(hashSize), ctx.args[3])
	err = ctx.db.MSet(keys, values)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt(1)
}

func hsetnxCommandFunc(ctx Context) {
	if len(ctx.args) != 4 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	metaValue, err := typeHashGetMeta(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	var hashSize uint32 = 0
	if metaValue != nil {
		hashSize = bytesToUint32(metaValue[1:5])
	}

	field := typeHashMarshalField(key, ctx.args[2])
	exists, err := ctx.db.Get(field)
	if err == nil && exists != nil {
		ctx.Conn.WriteInt(0)
		return
	}
	err = ctx.db.Set(field, ctx.args[3], 0)
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
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		} else {
			ctx.Conn.WriteNull()
			return
		}
	}

	field := typeHashMarshalField(key, ctx.args[2])
	v, err := ctx.db.Get(field)
	if err != nil {
		ctx.Conn.WriteNull()
		return
	}

	ctx.Conn.WriteBulk(v)
}

func hexistsCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgsN, len(ctx.args)-1, 2))
		return
	}

	var key = ctx.args[1]
	_, err := typeHashGetMeta(ctx, key)
	if err != nil {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		} else {
			ctx.Conn.WriteInt(0)
			return
		}
	}

	field := typeHashMarshalField(key, ctx.args[2])
	_, err = ctx.db.Get(field)
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
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		} else {
			ctx.Conn.WriteInt(0)
			return
		}
	}

	var fieldToDel [][]byte
	for _, f := range ctx.args[2:] {
		field := typeHashMarshalField(key, f)
		fieldToDel = append(fieldToDel, field)
	}

	var hashSize uint32 = 0
	if metaValue != nil {
		hashSize = bytesToUint32(metaValue[1:5])
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

func hincrbyCommandFunc(ctx Context) {
	if len(ctx.args) != 4 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgsN, len(ctx.args)-1, 3))
		return
	}

	var key             = ctx.args[1]
	metaValue, err := typeHashGetMeta(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}
	var hashSize = bytesToUint32(metaValue[1:5])

	field := typeHashMarshalField(key, ctx.args[2])
	val, err := ctx.db.Get(field)
	if err != nil {
		val = []byte("0")
	}

	valInt, _ := incrInt64ToByte(val, ctx.args[3])
	valStr := strconv.FormatInt(valInt, 10)
	err = ctx.db.Set(field, []byte(valStr), 0)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	if metaValue == nil && hashSize == 0 {
		hashSize += 1
		err = typeHashSetMeta(ctx, key, hashSize)
		if err != nil {
			ctx.Conn.WriteInt(0)
			return
		}
	}

	ctx.Conn.WriteInt64(valInt)
}

func hlenCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	metaValue, err := typeHashGetMeta(ctx, ctx.args[1])
	if err != nil {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		} else {
			ctx.Conn.WriteInt(0)
			return
		}
	}

	var hashSize uint32 = 0
	if metaValue != nil {
		hashSize = bytesToUint32(metaValue[1:5])
	}

	ctx.Conn.WriteInt(int(hashSize))
}

func hstrlenCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	_, err := typeHashGetMeta(ctx, ctx.args[1])
	if err != nil {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		} else {
			ctx.Conn.WriteInt(0)
			return
		}
	}

	field := typeHashMarshalField(ctx.args[1], ctx.args[2])
	v, err := ctx.db.Get(field)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt(len(string(v)))
}

func hgetallCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	_, err := typeHashGetMeta(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	var fieldPos = typeHashFieldPos(key)
	fields, values := typeHashScan(ctx, key, 0)
	ctx.Conn.WriteArray(len(fields) * 2)
	for i := 0; i < len(fields); i++ {
		ctx.Conn.WriteBulk(fields[i][fieldPos:])
		ctx.Conn.WriteBulk(values[i])
	}
}

func hmsetCommandFunc(ctx Context) {
	if len(ctx.args) < 3 || len(ctx.args)&1 != 0 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var (
		key             = ctx.args[1]
		hashSize uint32 = 0
	)
	metaValue, err := typeHashGetMeta(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	if metaValue != nil {
		hashSize = bytesToUint32(metaValue[1:5])
	}
	var cnt uint32
	for i := 0; i < len(ctx.args[2:]); i += 2 {
		field := typeHashMarshalField(key, ctx.args[2:][i])
		_, err := ctx.db.Get(field)
		if err == nil {
			continue
		}

		err = ctx.db.Set(field, ctx.args[2:][i+1], 0)
		if err == nil {
			cnt++
		}
	}

	hashSize += cnt
	err = typeHashSetMeta(ctx, key, hashSize)
	if err != nil {
		ctx.Conn.WriteError(err.Error())
		return
	}

	ctx.Conn.WriteString(RespOK)
}

func hmgetCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	_, err := typeHashGetMeta(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	var values [][]byte
	if err != nil && err.Error() == ErrKeyNotExist {
		for i := 0; i < len(ctx.args[2:]); i++ {
			values = append(values, nil)
		}
	} else {
		for _, f := range ctx.args[2:] {
			field := typeHashMarshalField(key, f)
			v, _ := ctx.db.Get(field)
			values = append(values, v)
		}
	}

	ctx.Conn.WriteArray(len(values))
	for i := 0; i < len(values); i++ {
		if values[i] == nil {
			ctx.Conn.WriteNull()
			continue
		}
		ctx.Conn.WriteBulk(values[i])
	}
}

func hkeysCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	_, err := typeHashGetMeta(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	var fieldPos = typeHashFieldPos(key)
	fields, _ := typeHashScan(ctx, key, 0)
	ctx.Conn.WriteArray(len(fields))
	for i := 0; i < len(fields); i++ {
		ctx.Conn.WriteBulk(fields[i][fieldPos:])
	}
}

func hvalsCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	_, err := typeHashGetMeta(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	_, values := typeHashScan(ctx, key, 0)
	ctx.Conn.WriteArray(len(values))
	for i := 0; i < len(values); i++ {
		ctx.Conn.WriteBulk(values[i])
	}
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
	return ctx.db.Set(key, typeHashMetaVal(size), 0)
}

func typeHashMetaVal(size uint32) []byte {
	return append(typeHash, uint32ToBytes(typeHashKeySize, size)...)
}

func typeHashMarshalField(key, field []byte) []byte {
	var keySize = uint32ToBytes(typeHashKeySize, uint32(len(key)))

	fieldBuff := bytes.NewBuffer([]byte{})
	fieldBuff.Write(typeHash)
	fieldBuff.Write(keySize)
	fieldBuff.Write(key)
	fieldBuff.Write(field)

	return fieldBuff.Bytes()
}

func typeHashScan(ctx Context, key []byte, cnt int64) ([][]byte, [][]byte) {
	var (
		fields   [][]byte
		values   [][]byte
		scanFunc = func(k, v []byte) {
			fields = append(fields, k)
			values = append(values, v)
		}
	)

	var keySize = uint32ToBytes(typeHashKeySize, uint32(len(key)))
	prefixBuff := bytes.NewBuffer([]byte{})
	prefixBuff.Write(typeHash)
	prefixBuff.Write(keySize)
	prefixBuff.Write(key)

	scanOpts := badger.ScannerOptions{
		Prefix:      prefixBuff.Bytes(),
		FetchValues: true,
		Handler:     scanFunc,
		Count:       cnt,
	}
	ctx.db.Scan(scanOpts)

	return fields, values
}

func typeHashFieldPos(key []byte) uint32 {
	return typeHashSize + typeHashKeySize + uint32(len(key))
}
