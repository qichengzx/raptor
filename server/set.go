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

var typeSet = byte(storage.ObjectSet)

func saddCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}
	var buff bytes.Buffer
	buff.WriteByte(typeSet)
	buff.Write(ctx.args[1])
	var metaKey = buff.Bytes()
	buff.Reset()

	metaValue, err := ctx.db.Get(metaKey)
	var setSize int64 = 0
	if err == nil && metaValue != nil {
		valueBuff := bytes.NewBuffer(metaValue)
		binary.Read(valueBuff, binary.BigEndian, &setSize)
	}

	var cnt int64 = 0
	var dataBuff bytes.Buffer
	memberSizeBuff := bytes.NewBuffer([]byte{})

	for _, member := range ctx.args[2:] {
		memberSizeBuff.Reset()
		binary.Write(memberSizeBuff, binary.BigEndian, len(member))
		memberSize := memberSizeBuff.Bytes()

		dataBuff.WriteByte(typeSet)
		dataBuff.Write(memberSize)
		dataBuff.Write(ctx.args[1])
		dataBuff.Write(member)
		var dataKey = dataBuff.Bytes()
		dataBuff.Reset()

		v, err := ctx.db.Get(dataKey)
		if err == nil && v != nil {
			continue
		}

		err = ctx.db.Set(dataKey, []byte{0}, 0)
		if err == nil {
			cnt++
		}
	}

	setSize += cnt
	sizeBuff := bytes.NewBuffer([]byte{})
	binary.Write(sizeBuff, binary.BigEndian, setSize)
	err = ctx.db.Set(metaKey, sizeBuff.Bytes(), 0)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt64(cnt)
}

func sismemberCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}
	var dataBuff bytes.Buffer
	memberSizeBuff := bytes.NewBuffer([]byte{})
	binary.Write(memberSizeBuff, binary.BigEndian, len(ctx.args[2]))
	memberSize := memberSizeBuff.Bytes()

	dataBuff.WriteByte(typeSet)
	dataBuff.Write(memberSize)
	dataBuff.Write(ctx.args[1])
	dataBuff.Write(ctx.args[2])

	v, err := ctx.db.Get(dataBuff.Bytes())

	if err == nil && v != nil {
		ctx.Conn.WriteInt(1)
		return
	}

	ctx.Conn.WriteInt(0)
}
