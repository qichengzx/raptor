package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/qichengzx/raptor/storage/badger"
	"strconv"
)

const (
	cmdSAdd        = "sadd"
	cmdSismember   = "sismember"
	cmdSPop        = "spop"
	cmdSrandmember = "srandmember"
	cmdSRem        = "srem"
	cmdSCard       = "scard"
	cmdSmembers    = "smembers"
)

const (
	typeSetMemberDefault = "1"
	typeSetKeySize       = 4
)

var (
	typeSet     = []byte("S")
	typeSetSize = uint32(len(typeSet))
)

func saddCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	var keyLen = uint32(len(key))
	var keySize = make([]byte, typeSetKeySize)
	var setSize uint32 = 0

	metaValue, err := ctx.db.Get(key)
	if err == nil && metaValue != nil {
		setSize = binary.BigEndian.Uint32(metaValue[1:5])
	}

	binary.BigEndian.PutUint32(keySize, keyLen)
	var cnt uint32 = 0
	var memDefault = []byte(typeSetMemberDefault)
	for _, member := range ctx.args[2:] {
		memBuff := bytes.NewBuffer([]byte{})
		memBuff.Write(typeSet)
		memBuff.Write(keySize)
		memBuff.Write(key)
		memBuff.Write(member)

		_, err := ctx.db.Get(memBuff.Bytes())
		if err == nil {
			continue
		}

		err = ctx.db.Set(memBuff.Bytes(), memDefault, 0)
		if err == nil {
			cnt++
		}
	}
	setSize += cnt

	var metaSize = make([]byte, typeSetKeySize)
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
	var keySize = make([]byte, typeSetKeySize)

	binary.BigEndian.PutUint32(keySize, keyLen)
	var member = ctx.args[2]

	memBuff := bytes.NewBuffer([]byte{})
	memBuff.Write(typeSet)
	memBuff.Write(keySize)
	memBuff.Write(key)
	memBuff.Write(member)

	v, err := ctx.db.Get(memBuff.Bytes())
	if err == nil && string(v) == typeSetMemberDefault {
		ctx.Conn.WriteInt(1)
		return
	}

	ctx.Conn.WriteInt(0)
}

func spopCommandFunc(ctx Context) {
	if len(ctx.args) != 2 && len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	metaValue, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		ctx.Conn.WriteNull()
		return
	}

	var setSize uint32 = 0
	if err == nil && metaValue != nil {
		setSize = binary.BigEndian.Uint32(metaValue[1:5])
	}

	var keyLen = uint32(len(key))
	var keySize = make([]byte, typeSetKeySize)
	binary.BigEndian.PutUint32(keySize, keyLen)

	var cnt int64 = 1
	if len(ctx.args) == 3 {
		cnt, err = strconv.ParseInt(string(ctx.args[2]), 10, 64)
		if err != nil {
			ctx.Conn.WriteError(ErrValue)
			return
		}
	}
	var memberPos = typeSetSize + typeSetKeySize + keyLen
	members := typeSetScan(ctx, key, cnt)
	var lenToDel = len(members)
	if lenToDel > 0 {
		memberToDel := members
		setSize -= uint32(lenToDel)
		if setSize == 0 {
			memberToDel = append(memberToDel, key)
		}

		ctx.db.Del(memberToDel)
	}

	if setSize > 0 {
		var metaSize = make([]byte, typeSetKeySize)
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
		ctx.Conn.WriteBulk(member[memberPos:])
	}
}

func srandmemberCommandFunc(ctx Context) {
	if len(ctx.args) != 2 && len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	_, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		ctx.Conn.WriteNull()
		return
	}

	var cnt int64 = 1
	if len(ctx.args) == 3 {
		cnt, err = strconv.ParseInt(string(ctx.args[2]), 10, 64)
		if err != nil {
			ctx.Conn.WriteError(ErrValue)
			return
		}
	}

	var memberPos = typeSetSize + typeSetKeySize + uint32(len(key))
	members := typeSetScan(ctx, key, cnt)
	if cnt == 1 {
		if len(members) == 1 {
			ctx.Conn.WriteBulk(members[0][memberPos:])
		} else {
			ctx.Conn.WriteNull()
		}
		return
	}

	ctx.Conn.WriteArray(len(members))
	for _, member := range members {
		ctx.Conn.WriteBulk(member[memberPos:])
	}
}

func sremCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	metaValue, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		ctx.Conn.WriteError(ErrWrongType)
		return
	}

	var setSize uint32 = 0
	if err == nil && metaValue != nil {
		setSize = binary.BigEndian.Uint32(metaValue[1:5])
	}

	var keyLen = uint32(len(key))
	var keySize = make([]byte, typeSetKeySize)
	binary.BigEndian.PutUint32(keySize, keyLen)
	var memberToDel [][]byte
	for _, member := range ctx.args[2:] {
		memBuff := bytes.NewBuffer([]byte{})
		memBuff.Write(typeSet)
		memBuff.Write(keySize)
		memBuff.Write(key)
		memBuff.Write(member)

		v, err := ctx.db.Get(memBuff.Bytes())
		if err == nil && string(v) == typeSetMemberDefault {
			memberToDel = append(memberToDel, memBuff.Bytes())
			continue
		}
	}

	var lenToDel = len(memberToDel)
	if lenToDel > 0 {
		setSize -= uint32(lenToDel)
		if setSize == 0 {
			memberToDel = append(memberToDel, key)
		}

		ctx.db.Del(memberToDel)
		if setSize == 0 {
			ctx.Conn.WriteInt(lenToDel)
			return
		}
	}

	var metaSize = make([]byte, typeSetKeySize)
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
	_, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		ctx.Conn.WriteError(ErrEmpty)
		return
	}

	var memberPos = typeSetSize + typeSetKeySize + uint32(len(key))
	members := typeSetScan(ctx, key, 0)
	ctx.Conn.WriteArray(len(members))
	for _, member := range members {
		ctx.Conn.WriteBulk(member[memberPos:])
	}
}

func typeSetScan(ctx Context, key []byte, cnt int64) [][]byte {
	var members [][]byte
	var scanFunc = func(k, v []byte) {
		members = append(members, k)
	}

	var keySize = make([]byte, typeSetKeySize)
	var keyLen = uint32(len(key))
	binary.BigEndian.PutUint32(keySize, keyLen)

	prefixBuff := bytes.NewBuffer([]byte{})
	prefixBuff.Write(typeSet)
	prefixBuff.Write(keySize)
	prefixBuff.Write(key)

	scanOpts := badger.ScannerOptions{
		Prefix:      prefixBuff.Bytes(),
		FetchValues: false,
		Handler:     scanFunc,
		Count:       cnt,
	}
	ctx.db.Scan(scanOpts)

	return members
}
