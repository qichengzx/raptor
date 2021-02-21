package server

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/qichengzx/raptor/storage/badger"
	"strconv"
)

const (
	cmdZAdd    = "zadd"
	cmdZScore  = "zscore"
	cmdZIncrby = "zincrby"
	cmdZCard   = "zcard"
	cmdZRem    = "zrem"
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

	metaValue, err := typeZSetGetMeta(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	if metaValue != nil {
		zsetSize = bytesToUint32(metaValue[1:5])
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

func zscoreCommandFunc(ctx Context) {
	if len(ctx.args) != 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	_, err := typeZSetGetMeta(ctx, ctx.args[1])
	if err != nil {
		if err.Error() == ErrKeyNotExist {
			ctx.Conn.WriteNull()
			return
		}
		ctx.Conn.WriteError(err.Error())
		return
	}

	member := typeZSetMarshalMemeber(ctx.args[1], ctx.args[2])
	score, err := ctx.db.Get(member)
	if err != nil {
		ctx.Conn.WriteNull()
		return
	}

	ctx.Conn.WriteBulk(score)
}

func zincrbyCommandFunc(ctx Context) {
	if len(ctx.args) != 4 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	member := typeZSetMarshalMemeber(key, ctx.args[3])
	score, err := ctx.db.Get(member)
	scoreStr := string(score)
	if err != nil {
		scoreStr = "0"
	}

	scoreInt, err := strconv.ParseInt(scoreStr, 10, 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}
	incr, err := strconv.ParseInt(string(ctx.args[2]), 10, 64)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}
	scoreInt += incr
	scoreStr = strconv.FormatInt(scoreInt, 10)

	err = ctx.db.Set(member, score, 0)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}
	score = typeZSetMarshalScore(key, []byte(scoreStr), ctx.args[3])
	err = ctx.db.Set(score, nil, 0)
	if err != nil {
		ctx.Conn.WriteError(ErrValue)
		return
	}
	ctx.Conn.WriteBulk([]byte(scoreStr))
}

func zcardCommandFunc(ctx Context) {
	if len(ctx.args) != 2 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	metaValue, err := typeZSetGetMeta(ctx, ctx.args[1])
	if err != nil {
		if err.Error() != ErrKeyNotExist {
			ctx.Conn.WriteError(err.Error())
			return
		}
		ctx.Conn.WriteInt(0)
		return
	}

	var zsetSize uint32 = 0
	if metaValue != nil {
		zsetSize = bytesToUint32(metaValue[1:5])
	}

	ctx.Conn.WriteInt(int(zsetSize))
}

func zremCommandFunc(ctx Context) {
	if len(ctx.args) < 3 {
		ctx.Conn.WriteError(fmt.Sprintf(ErrWrongArgs, ctx.cmd))
		return
	}

	var key = ctx.args[1]
	metaValue, err := typeZSetGetMeta(ctx, key)
	if err != nil && err.Error() != ErrKeyNotExist {
		ctx.Conn.WriteError(err.Error())
		return
	}

	var zsetSize uint32 = 0
	if metaValue != nil {
		zsetSize = bytesToUint32(metaValue[1:5])
	}

	var cnt uint32 = 0
	var keysToDel [][]byte
	for i := 2; i <= len(ctx.args[1:]); i++ {
		member := typeZSetMarshalMemeber(key, ctx.args[i])
		score, err := ctx.db.Get(member)
		if err != nil {
			continue
		}
		scoreKey := typeZSetMarshalScore(key, score, ctx.args[i])
		keysToDel = append(keysToDel, member, scoreKey)
		cnt++
	}

	zsetSize -= cnt
	if zsetSize == 0 {
		keysToDel = append(keysToDel, key)
	}
	err = ctx.db.Del(keysToDel)
	if zsetSize > 0 {
		err = typeZSetSetMeta(ctx, key, zsetSize)
		if err != nil {
			ctx.Conn.WriteInt(0)
			return
		}
	}

	ctx.Conn.WriteInt64(int64(cnt))
}

func typeZSetScan(ctx Context, key []byte, cnt int64) ([][]byte, [][]byte) {
	var members [][]byte
	var score [][]byte
	var scanFunc = func(k, v []byte) {
		members = append(members, k)
		score = append(score, v)
	}

	var keySize = uint32ToBytes(typeZSetKeySize, uint32(len(key)))
	prefixBuff := bytes.NewBuffer([]byte{})
	prefixBuff.Write(typeZSet)
	prefixBuff.Write(keySize)
	prefixBuff.Write(key)

	scanOpts := badger.ScannerOptions{
		Prefix:      prefixBuff.Bytes(),
		FetchValues: true,
		Handler:     scanFunc,
		Count:       cnt,
	}
	ctx.db.Scan(scanOpts)

	return members, score
}

func typeZSetMarshalMemeber(key, member []byte) []byte {
	var keySize = uint32ToBytes(typeZSetKeySize, uint32(len(key)))
	memberBuff := bytes.NewBuffer([]byte{})
	memberBuff.Write(typeZSet)
	memberBuff.Write(keySize)
	memberBuff.Write(key)
	memberBuff.Write(member)

	return memberBuff.Bytes()
}

func typeZSetMarshalScore(key, score, member []byte) []byte {
	var keySize = uint32ToBytes(typeZSetKeySize, uint32(len(key)))
	scoreBuff := bytes.NewBuffer([]byte{})
	scoreBuff.Write(typeZSet)
	scoreBuff.Write(keySize)
	scoreBuff.Write(key)
	scoreBuff.Write(score)
	scoreBuff.Write(member)

	return scoreBuff.Bytes()
}

func typeZSetGetMeta(ctx Context, key []byte) ([]byte, error) {
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
	return ctx.db.Set(key, append(typeZSet, uint32ToBytes(typeZSetKeySize, size)...), 0)
}
