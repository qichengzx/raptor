package server

import (
	"github.com/qichengzx/raptor/raptor"
	"github.com/tidwall/redcon"
)

type CommandHandler func(c Context)

type Context struct {
	redcon.Conn
	db   *raptor.Raptor
	cmd  string
	args [][]byte
}

var (
	commands = map[string]CommandHandler{
		//STRING
		cmdSet:         setCommandFunc,
		cmdSetNX:       setnxCommandFunc,
		cmdSetEX:       setexCommandFunc,
		cmdPSetEX:      psetexCommandFunc,
		cmdGet:         getCommandFunc,
		cmdGetSet:      getsetCommandFunc,
		cmdStrlen:      strlenCommandFunc,
		cmdAppend:      appendCommandFunc,
		cmdIncr:        incrCommandFunc,
		cmdIncrBy:      incrByCommandFunc,
		cmdDecr:        decrCommandFunc,
		cmdDecrBy:      decrByCommandFunc,
		cmdIncrByFloat: incrByFloatCommandFunc,
		cmdMSet:        msetCommandFunc,
		cmdMSetNX:      msetnxCommandFunc,
		cmdMGet:        mgetCommandFunc,

		//SET
		cmdSAdd:        saddCommandFunc,
		cmdSismember:   sismemberCommandFunc,
		cmdSPop:        spopCommandFunc,
		cmdSrandmember: srandmemberCommandFunc,
		cmdSRem:        sremCommandFunc,
		cmdSCard:       scardCommandFunc,
		cmdSmembers:    smembersCommandFunc,

		//DATABASE
		cmdSelect:   selectCommandFunc,
		cmdDel:      delCommandFunc,
		cmdExists:   existsCommandFunc,
		cmdRename:   renameCommandFunc,
		cmdRenameNX: renamenxCommandFunc,
		cmdFlushDB:  flushdbCommandFunc,
		cmdFlushAll: flushallCommandFunc,
		cmdType:     typeCommandFunc,

		//EXPIRE
		cmdExpire:   expireCommandFunc,
		cmdPExpire:  pexpireCommandFunc,
		cmdExpireAt: expireatCommandFunc,
		cmdTTL:      ttlCommandFunc,
		cmdPersist:  persistCommandFunc,

		//DEBUG
		cmdPing: pingCommandFunc,
		cmdEcho: echoCommandFunc,
	}
)
