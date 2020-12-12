package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/qichengzx/raptor/storage/badger"
	"strconv"
)

const (
	cmdSAdd      = "sadd"
	cmdSismember = "sismember"
	cmdSPop      = "spop"
	cmdSRem      = "srem"
	cmdSCard     = "scard"
	cmdSmembers  = "smembers"
)

const memberDefault = "1"

var typeSet = []byte("S")

func saddCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	var keyLen = uint32(len(key))
	var keySize = make([]byte, 4)
	var setSize uint32 = 0

	metaValue, err := ctx.db.Get(key)
	if err == nil && metaValue != nil {
		setSize = binary.BigEndian.Uint32(metaValue[1:5])
	}

	binary.BigEndian.PutUint32(keySize, keyLen)
	var cnt uint32 = 0
	var memDefault = []byte(memberDefault)
	for _, member := range ctx.args[2:] {
		memBuff := bytes.NewBuffer([]byte{})
		memBuff.Write(typeSet)
		memBuff.Write(keySize)
		memBuff.Write(key)
		memBuff.Write(member)

		v, err := ctx.db.Get(memBuff.Bytes())
		if err == nil && string(v) == memberDefault {
			continue
		}

		err = ctx.db.Set(memBuff.Bytes(), memDefault, 0)
		if err == nil {
			cnt++
		}
	}
	setSize += cnt

	var metaSize = make([]byte, 4)
	binary.BigEndian.PutUint32(metaSize, setSize)
	metaValue = metaSize
	err = ctx.db.Set(key, append(typeSet, metaValue...), 0)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt64(int64(cnt))
	return
}

func sismemberCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	var keyLen = uint32(len(key))
	var keySize = make([]byte, 4)

	binary.BigEndian.PutUint32(keySize, keyLen)
	var member = ctx.args[2]

	memBuff := bytes.NewBuffer([]byte{})
	memBuff.Write(typeSet)
	memBuff.Write(keySize)
	memBuff.Write(key)
	memBuff.Write(member)

	v, err := ctx.db.Get(memBuff.Bytes())
	if err == nil && string(v) == memberDefault {
		ctx.Conn.WriteInt(1)
		return
	}

	ctx.Conn.WriteInt(0)
}

func spopCommandFunc(ctx Context) {
	if len(ctx.args) != 2 || len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	var keyLen = uint32(len(key))
	var keySize = make([]byte, 4)
	var setSize uint32 = 0

	metaValue, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		ctx.Conn.WriteNull()
		return
	}
	if err == nil && metaValue != nil {
		setSize = binary.BigEndian.Uint32(metaValue[1:5])
	}

	binary.BigEndian.PutUint32(keySize, keyLen)

	prefixBuff := bytes.NewBuffer([]byte{})
	prefixBuff.Write(typeSet)
	prefixBuff.Write(keySize)
	prefixBuff.Write(key)

	var members [][]byte
	var memberToDel [][]byte
	var memberPos = uint32(len(typeSet)) + uint32(len(keySize)) + keyLen
	var scanFunc = func(k, v []byte) {
		memberToDel = append(memberToDel, k)
		members = append(members, k[memberPos:])
	}

	scanOpts := badger.ScannerOptions{
		Prefix:      prefixBuff.Bytes(),
		FetchValues: false,
		Handler:     scanFunc,
		Count: 1,
	}

	if len(ctx.args) == 3 {
		cnt, err := strconv.ParseInt(string(ctx.args[2]), 10, 64)
		if err != nil {
			ctx.Conn.WriteError(ErrValue)
			return
		}
		scanOpts.Count = cnt
	}
	ctx.db.Scan(scanOpts)

	var lenToDel = len(memberToDel)
	if lenToDel > 0 {
		ctx.db.Del(memberToDel)
		setSize -= uint32(lenToDel)
	}

	if setSize == 0 {
		var del [][]byte
		del = append(del, key)
		ctx.db.Del(del)
	} else {
		var metaSize = make([]byte, 4)
		binary.BigEndian.PutUint32(metaSize, setSize)
		metaValue = metaSize
		err = ctx.db.Set(key, append(typeSet, metaValue...), 0)
		if err != nil {
			ctx.Conn.WriteInt(0)
			return
		}
	}

	ctx.Conn.WriteArray(len(members))
	for _, member := range members {
		ctx.Conn.WriteBulk(member)
	}
}

func sremCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	var keyLen = uint32(len(key))
	var keySize = make([]byte, 4)
	var setSize uint32 = 0

	metaValue, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		ctx.Conn.WriteError(ErrWrongType)
		return
	}
	if err == nil && metaValue != nil {
		setSize = binary.BigEndian.Uint32(metaValue[1:5])
	}

	binary.BigEndian.PutUint32(keySize, keyLen)
	var memberToDel [][]byte
	for _, member := range ctx.args[2:] {
		memBuff := bytes.NewBuffer([]byte{})
		memBuff.Write(typeSet)
		memBuff.Write(keySize)
		memBuff.Write(key)
		memBuff.Write(member)

		v, err := ctx.db.Get(memBuff.Bytes())
		if err == nil && string(v) == memberDefault {
			memberToDel = append(memberToDel, memBuff.Bytes())
			continue
		}
	}

	var lenToDel = len(memberToDel)
	if lenToDel > 0 {
		err = ctx.db.Del(memberToDel)
		setSize -= uint32(lenToDel)
	}

	if setSize == 0 {
		var del = make([][]byte, 1)
		del = append(del, key)
		ctx.db.Del(del)
		ctx.Conn.WriteInt(lenToDel)
		return
	}

	var metaSize = make([]byte, 4)
	binary.BigEndian.PutUint32(metaSize, setSize)
	metaValue = metaSize
	err = ctx.db.Set(key, append(typeSet, metaValue...), 0)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt(lenToDel)
	return
}

func scardCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	metaValue, _ := ctx.db.Get(ctx.args[1])
	var setSize uint32 = 0
	if metaValue != nil {
		setSize = binary.BigEndian.Uint32(metaValue[1:5])
	}

	ctx.Conn.WriteInt(int(setSize))
	return
}

func smembersCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	var keyLen = uint32(len(key))
	var keySize = make([]byte, 4)

	_, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		ctx.Conn.WriteError(ErrEmpty)
		return
	}

	binary.BigEndian.PutUint32(keySize, keyLen)

	prefixBuff := bytes.NewBuffer([]byte{})
	prefixBuff.Write(typeSet)
	prefixBuff.Write(keySize)
	prefixBuff.Write(key)

	var members [][]byte
	var memberPos = uint32(len(typeSet)) + uint32(len(keySize)) + keyLen
	var scanFunc = func(k, v []byte) {
		members = append(members, k[memberPos:])
	}

	scanOpts := badger.ScannerOptions{
		Prefix:      prefixBuff.Bytes(),
		FetchValues: false,
		Handler:     scanFunc,
	}

	ctx.db.Scan(scanOpts)

	ctx.Conn.WriteArray(len(members))
	for _, member := range members {
		ctx.Conn.WriteBulk(member)
	}
}
