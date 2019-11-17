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
		cmdSet:    setCommandFunc,
		cmdSetNX:  setnxCommandFunc,
		cmdGet:    getCommandFunc,
		cmdGetSet: getsetCommandFunc,
		cmdStrlen: strlenCommandFunc,
		cmdAppend: appendCommandFunc,
		cmdIncr:   incrCommandFunc,
		cmdIncrBy: incrByCommandFunc,
		cmdDecr:   decrCommandFunc,
		cmdDecrBy: decrByCommandFunc,
		cmdMSet:   msetCommandFunc,
		cmdMGet:   mgetCommandFunc,

		//DATABASE
		cmdDel:    delCommandFunc,
		cmdExists: existsCommandFunc,
		cmdRename: renameCommandFunc,

		//EXPIRE
		cmdExpire: expireCommandFunc,

		//DEBUG
		cmdPing: pingCommandFunc,
		cmdEcho: echoCommandFunc,
	}
)
