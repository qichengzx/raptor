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
	cmdSUnionStore = "sunionstore"
	cmdSDiff       = "sdiff"
	cmdSDiffStore  = "sdiffstore"
)

const (
	typeSetMemberDefault = "1"
	typeSetKeySize       = 4
)

var (
	typeSetMemberDefaultByte = []byte(typeSetMemberDefault)
	typeSet                  = []byte("S")
	typeSetSize              = uint32(len(typeSet))
)

func saddCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
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

	binary.BigEndian.PutUint32(keySize, uint32(len(key)))
	var cnt uint32 = 0
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

		err = ctx.db.Set(memBuff.Bytes(), typeSetMemberDefaultByte, 0)
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
	var keySize = make([]byte, typeSetKeySize)
	binary.BigEndian.PutUint32(keySize, uint32(len(key)))

	memBuff := bytes.NewBuffer([]byte{})
	memBuff.Write(typeSet)
	memBuff.Write(keySize)
	memBuff.Write(key)
	memBuff.Write(ctx.args[2])

	v, err := ctx.db.Get(memBuff.Bytes())
	if err == nil && string(v) == typeSetMemberDefault {
		ctx.Conn.WriteInt(1)
		return
	}

	ctx.Conn.WriteInt(0)
}

func spopCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
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

	var cnt int64 = 1
	if len(ctx.args) == 3 {
		cnt, err = strconv.ParseInt(string(ctx.args[2]), 10, 64)
		if err != nil {
			ctx.Conn.WriteError(ErrValue)
			return
		}
	}
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
	var memberPos = typeSetMemberPos(key)
	for _, member := range members {
		ctx.Conn.WriteBulk(member[memberPos:])
	}
}

func srandmemberCommandFunc(ctx Context) {
	if len(ctx.args) != 2 || len(ctx.args) != 3 {
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

	var memberPos = typeSetMemberPos(key)
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

	var keySize = make([]byte, typeSetKeySize)
	binary.BigEndian.PutUint32(keySize, uint32(len(key)))
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
	meta, err := typeSetGetMeta(ctx, ctx.args[1])
	if err != nil {
		if err.Error() != ErrKeyNotExist && err.Error() != ErrWrongType {
			ctx.Conn.WriteError(err.Error())
			return
		}
	}
	if meta == nil {
		ctx.Conn.WriteError(ErrEmpty)
		return
	}

	var memberPos = typeSetMemberPos(key)
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

		var memberPos = typeSetMemberPos(key)
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

func sunionstoreCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var union [][]byte
	var check = make(map[string]struct{})

	for _, key := range ctx.args[2:] {
		var memberPos = typeSetMemberPos(key)
		members := typeSetScan(ctx, key, 0)
		for _, member := range members {
			if _, ok := check[string(member[memberPos:])]; !ok {
				check[string(member[memberPos:])] = struct{}{}
				union = append(union, member[memberPos:])
			}
		}
	}

	if len(union) == 0 {
		ctx.Conn.WriteError(ErrEmpty)
		return
	}

	var dstkey = ctx.args[1]
	keys := scan(ctx, dstkey)
	if len(keys) > 0 {
		var keysToDel [][]byte
		keysToDel = append(keysToDel, keys...)
		err := ctx.db.Del(keysToDel)
		if err != nil {
			ctx.Conn.WriteInt(0)
			return
		}
	}

	var keySize = make([]byte, typeSetKeySize)
	binary.BigEndian.PutUint32(keySize, uint32(len(dstkey)))
	var cnt uint32 = 0
	for _, member := range union {
		memBuff := bytes.NewBuffer([]byte{})
		memBuff.Write(typeSet)
		memBuff.Write(keySize)
		memBuff.Write(dstkey)
		memBuff.Write(member)

		err := ctx.db.Set(memBuff.Bytes(), typeSetMemberDefaultByte, 0)
		if err == nil {
			cnt++
		}
	}
	err := typeSetSaveMeta(ctx, dstkey, cnt)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt(len(union))
}

func sdiffCommandFunc(ctx Context) {
	if len(ctx.args) < 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var check = make(map[string]int)
	for _, key := range ctx.args[1:] {
		var memberPos = typeSetMemberPos(key)
		members := typeSetScan(ctx, key, 0)
		for _, member := range members {
			check[string(member[memberPos:])]++
		}
	}

	var diff [][]byte
	for member, cnt := range check {
		if cnt == 1 {
			diff = append(diff, []byte(member))
		}
	}

	if len(diff) == 0 {
		ctx.Conn.WriteError(ErrEmpty)
		return
	}

	ctx.WriteArray(len(diff))
	for _, member := range diff {
		ctx.Conn.WriteBulk(member)
	}
}

func sdiffstoreCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var check = make(map[string]int)
	for _, key := range ctx.args[2:] {
		var memberPos = typeSetMemberPos(key)
		members := typeSetScan(ctx, key, 0)
		for _, member := range members {
			check[string(member[memberPos:])]++
		}
	}

	var diff []string
	for member, cnt := range check {
		if cnt == 1 {
			diff = append(diff, member)
		}
	}

	if len(diff) == 0 {
		ctx.Conn.WriteError(ErrEmpty)
		return
	}

	var dstkey = ctx.args[1]
	keys := scan(ctx, dstkey)
	if len(keys) > 0 {
		var keysToDel [][]byte
		keysToDel = append(keysToDel, keys...)
		err := ctx.db.Del(keysToDel)
		if err != nil {
			ctx.Conn.WriteInt(0)
			return
		}
	}

	var keySize = make([]byte, typeSetKeySize)
	binary.BigEndian.PutUint32(keySize, uint32(len(dstkey)))
	var cnt uint32 = 0
	for _, member := range diff {
		memBuff := bytes.NewBuffer([]byte{})
		memBuff.Write(typeSet)
		memBuff.Write(keySize)
		memBuff.Write(dstkey)
		memBuff.WriteString(member)

		err := ctx.db.Set(memBuff.Bytes(), typeSetMemberDefaultByte, 0)
		if err == nil {
			cnt++
		}
	}
	err := typeSetSaveMeta(ctx, dstkey, cnt)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt(len(diff))
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

func typeSetMemberPos(key []byte) uint32 {
	return typeSetSize + typeSetKeySize + uint32(len(key))
}
