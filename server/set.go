package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/qichengzx/raptor/storage/badger"
	"strconv"
)

const (
	cmdSAdd        = "sadd"
	cmdSIsmember   = "sismember"
	cmdSPop        = "spop"
	cmdSRandmember = "srandmember"
	cmdSRem        = "srem"
	cmdSCard       = "scard"
	cmdSMembers    = "smembers"
	cmdSUnion      = "sunion"
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

	metaValue, err := typeSetGetMeta(ctx, key)
	if err == nil && metaValue != nil {
		setSize = binary.BigEndian.Uint32(metaValue[1:5])
	} else {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		}
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

	err = typeSetSaveMeta(ctx, key, setSize)
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
	_, err := typeSetGetMeta(ctx, key)
	if err != nil {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		}
		ctx.Conn.WriteInt(0)
		return
	}
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
	metaValue, err := typeSetGetMeta(ctx, key)
	if err != nil {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		}
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
		err = typeSetSaveMeta(ctx, key, setSize)
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
	_, err := typeSetGetMeta(ctx, key)
	if err != nil {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		}
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
	metaValue, err := typeSetGetMeta(ctx, key)
	if err != nil {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		}
		ctx.Conn.WriteError(ErrEmpty)
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

	err = typeSetSaveMeta(ctx, key, setSize)
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

	metaValue, err := typeSetGetMeta(ctx, ctx.args[1])
	if err != nil {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		}
		ctx.Conn.WriteInt(0)
		return
	}

	var setSize uint32 = 0
	if metaValue != nil {
		setSize = binary.BigEndian.Uint32(metaValue[1:5])
	}

	ctx.Conn.WriteInt(int(setSize))
}

func smembersCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	_, err := typeSetGetMeta(ctx, ctx.args[1])
	if err != nil {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		}
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

func sunionCommandFunc(ctx Context) {
	if len(ctx.args) < 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var union [][]byte
	var check = map[string]bool{}

	for _, key := range ctx.args[1:] {
		_, err := typeSetGetMeta(ctx, key)
		if err != nil {
			if err.Error() != ErrKeyNotExist {
				ctx.Conn.WriteError(err.Error())
				return
			}
		}

		var memberPos = typeSetSize + typeSetKeySize + uint32(len(key))
		members := typeSetScan(ctx, key, 0)
		for _, member := range members {
			if _, ok := check[string(member[memberPos:])]; !ok {
				check[string(member[memberPos:])] = true
				union = append(union, member[memberPos:])
			}
		}
	}

	if len(union) == 0 {
		ctx.Conn.WriteError(ErrEmpty)
		return
	}

	ctx.WriteArray(len(union))
	for _, member := range union {
		ctx.Conn.WriteBulk(member)
	}
}

func typeSetGetMeta(ctx Context, key []byte) ([]byte, error) {
	metaValue, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		return nil, err
	}

	if metaValue != nil && len(metaValue) > 1 {
		if string(metaValue[0]) != string(typeSet) {
			return nil, errors.New(ErrWrongType)
		}
	}

	return metaValue, nil
}

func typeSetSaveMeta(ctx Context, key []byte, size uint32) error {
	var metaValue = make([]byte, typeSetKeySize)
	binary.BigEndian.PutUint32(metaValue, size)

	return ctx.db.Set(key, append(typeSet, metaValue...), 0)
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
