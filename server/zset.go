package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	cmdZAdd = "zadd"
)

const (
	typeZSetKeySize = 4
)

var (
	typeZSet     = []byte("Z")
	typeZSetSize = uint32(len(typeZSet))
)

func zaddCommandFunc(ctx Context) {
	if len(ctx.args) < 4 || len(ctx.args)&1 != 0 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	var zsetSize uint32 = 0

	metaValue, err := typeZSetGetMeata(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	if metaValue != nil {
		zsetSize = binary.BigEndian.Uint32(metaValue[1:5])
	}

	var cnt uint32 = 0
	for i := 2; i <= len(ctx.args[1:]); i += 2 {
		member := typeZSetMarshalMemeber(key, ctx.args[i+1])
		_, err := ctx.db.Get(member)
		if err == nil {
			continue
		}
		err = ctx.db.Set(member, ctx.args[i], 0)
		if err == nil {
			cnt++
		}
		score := typeZSetMarshalScore(key, ctx.args[i], ctx.args[i+1])
		err = ctx.db.Set(score, nil, 0)
	}

	zsetSize += cnt
	err = typeZSetSetMeta(ctx, key, zsetSize)
	if err != nil {
		ctx.Conn.WriteInt(0)
		return
	}

	ctx.Conn.WriteInt64(int64(cnt))
}

func typeZSetMarshalMemeber(key, member []byte) []byte {
	var keySize = make([]byte, typeZSetKeySize)
	binary.BigEndian.PutUint32(keySize, uint32(len(key)))

	memberBuff := bytes.NewBuffer([]byte{})
	memberBuff.Write(typeZSet)
	memberBuff.Write(keySize)
	memberBuff.Write(key)
	memberBuff.Write(member)

	return memberBuff.Bytes()
}

func typeZSetMarshalScore(key, score, member []byte) []byte {
	var keySize = make([]byte, typeZSetKeySize)
	binary.BigEndian.PutUint32(keySize, uint32(len(key)))

	scoreBuff := bytes.NewBuffer([]byte{})
	scoreBuff.Write(typeZSet)
	scoreBuff.Write(keySize)
	scoreBuff.Write(key)
	scoreBuff.Write(score)
	scoreBuff.Write(member)

	return scoreBuff.Bytes()
}

func typeZSetGetMeata(ctx Context, key []byte) ([]byte, error) {
	metaValue, err := ctx.db.Get(key)
	if err != nil && err.Error() == ErrKeyNotExist {
		return nil, err
	}

	if metaValue != nil && len(metaValue) > 1 {
		if string(metaValue[0]) != string(typeZSet) {
			return nil, errors.New(ErrWrongType)
		}
	}

	return metaValue, nil
}

func typeZSetSetMeta(ctx Context, key []byte, size uint32) error {
	var metaValue = make([]byte, typeZSetKeySize)
	binary.BigEndian.PutUint32(metaValue, size)

	return ctx.db.Set(key, append(typeZSet, metaValue...), 0)
}
