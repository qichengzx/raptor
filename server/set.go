package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/qichengzx/raptor/storage"
)

const (
	cmdSAdd      = "sadd"
	cmdSismember = "sismember"
)

const memberDefault = "1"

var typeSet = byte(storage.ObjectSet)

func marshalSetKey(key []byte) []byte {
	var buff bytes.Buffer
	buff.WriteByte(typeSet)
	buff.Write(key)

	return buff.Bytes()
}

func saddCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	var keyLen = uint32(len(key))
	var keySize = make([]byte, 4)
	var metaKey = marshalSetKey(key)
	var setSize uint32 = 0

	metaValue, err := ctx.db.Get(metaKey)
	if err == nil && metaValue != nil {
		setSize = binary.BigEndian.Uint32(metaValue[:4])
	}

	binary.BigEndian.PutUint32(keySize, keyLen)
	var cnt uint32 = 0
	var memDefault = []byte(memberDefault)
	for _, member := range ctx.args[2:] {
		memBuff := bytes.NewBuffer([]byte{})
		memBuff.WriteByte(typeSet)
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
	err = ctx.db.Set(metaKey, metaValue, 0)
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
	memBuff.WriteByte(typeSet)
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
