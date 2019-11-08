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
		cmdGet:    getCommandFunc,
		cmdStrlen: strlenCommandFunc,
		cmdIncr:   incrCommandFunc,
		cmdIncrBy: incrByCommandFunc,
		cmdDecr:   decrCommandFunc,
		cmdDecrBy: decrByCommandFunc,
		cmdMSet:   msetCommandFunc,

		//DATABASE
		cmdDel:    delCommandFunc,
		cmdExists: existsCommandFunc,
	}
)
