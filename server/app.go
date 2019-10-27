package server

import (
	"fmt"
	"github.com/qichengzx/raptor/config"
	"github.com/qichengzx/raptor/raptor"
	"github.com/tidwall/redcon"
	"log"
	"strings"
)

type App struct {
	conf   *config.Config
	db     *raptor.Raptor
	authed bool

	infoClients infoClients
}

type infoClients struct {
	connections int
}

func New(conf *config.Config) *App {
	db, err := raptor.New(conf)
	if err != nil {
		log.Fatal(err)
	}

	return &App{
		conf: conf,
		db:   db,
	}
}

func (app *App) Run() {
	addr := fmt.Sprintf("%s:%d", app.conf.Raptor.Host, app.conf.Raptor.Port)
	log.Printf("started server at :%d", app.conf.Raptor.Port)
	err := redcon.ListenAndServe(addr,
		app.onCommand(),
		app.onAccept(),
		app.onClose(),
	)
	if err != nil {
		log.Fatal(err)
	}
}

func (app *App) onCommand() func(conn redcon.Conn, cmd redcon.Command) {
	return func(conn redcon.Conn, cmd redcon.Command) {
		todo := strings.TrimSpace(strings.ToLower(string(cmd.Args[0])))

		switch todo {
		case "quit":
			app.infoClients.connections--
			conn.WriteString(RespOK)
			conn.Close()
		case "auth":
			if len(cmd.Args) != 2 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}

			if app.auth(string(cmd.Args[1])) {
				conn.WriteString(RespOK)
			} else {
				conn.WriteError(ErrPassword)
			}
		default:
			if !app.authed {
				conn.WriteError(ErrNoAuth)
				return
			}
			f, ok := commands[todo]
			if !ok {
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
				return
			}

			f(Context{
				Conn: conn,
				db:   app.db,
				cmd:  todo,
				args: cmd.Args,
			})
			return
		}
	}
}

func (app *App) onAccept() func(conn redcon.Conn) bool {
	return func(conn redcon.Conn) bool {
		log.Printf("accept: %s", conn.RemoteAddr())
		app.infoClients.connections++
		return true
	}
}

func (app *App) onClose() func(conn redcon.Conn, err error) {
	return func(conn redcon.Conn, err error) {
		log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		app.infoClients.connections--
	}
}

func (app *App) GetDB() *raptor.Raptor {
	return app.db
}

func (app *App) authCheck() bool {
	return app.authed
}

func (app *App) auth(auth string) bool {
	if auth == app.conf.Raptor.Auth {
		app.authed = true
	}

	return app.authed
}

func (app *App) Close() error {
	return app.db.Close()
}
