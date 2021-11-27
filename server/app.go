package server

import (
	"fmt"
	"github.com/qichengzx/raptor/config"
	"github.com/qichengzx/raptor/raptor"
	"github.com/tidwall/redcon"
	"log"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

type App struct {
	conf   *config.Config
	db     *raptor.Raptor
	authed map[string]struct{}

	infoServer  infoServer
	infoClients struct {
		connections int32
	}
	infoStat struct {
		totalConnectionsReceived int32
		totalCommandsProcessed   int32
	}
}

type infoServer struct {
	os string
	//archBits        int
	processID       int
	tcpPort         int
	uptimeInSeconds int
	uptime          time.Time
	uptimeInDays    int
}

func New(conf *config.Config) *App {
	db, err := raptor.New(conf)
	if err != nil {
		log.Fatal(err)
	}

	return &App{
		conf:   conf,
		db:     db,
		authed: make(map[string]struct{}),
		infoServer: infoServer{
			os:              runtime.GOOS,
			processID:       os.Getpid(),
			tcpPort:         conf.Raptor.Port,
			uptimeInSeconds: 0,
			uptime:          time.Now(),
			uptimeInDays:    0,
		},
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
				conn.WriteError(fmt.Sprintf(ErrWrongArgs, todo))
				return
			}

			if app.auth(conn, string(cmd.Args[1])) {
				conn.WriteString(RespOK)
			} else {
				conn.WriteError(ErrPassword)
			}
		case "info":

		default:
			atomic.AddInt32(&app.infoStat.totalCommandsProcessed, 1)
			if !app.authCheck(conn) {
				conn.WriteError(ErrNoAuth)
				return
			}
			f, ok := commands[todo]
			if !ok {
				conn.WriteError(fmt.Sprintf(ErrCmd, string(cmd.Args[0])))
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
		atomic.AddInt32(&app.infoClients.connections, 1)
		atomic.AddInt32(&app.infoStat.totalConnectionsReceived, 1)
		return true
	}
}

func (app *App) onClose() func(conn redcon.Conn, err error) {
	return func(conn redcon.Conn, err error) {
		log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		atomic.AddInt32(&app.infoClients.connections, -1)
	}
}

func (app *App) GetDB() *raptor.Raptor {
	return app.db
}

func (app *App) authCheck(conn redcon.Conn) bool {
	if _, ok := app.authed[conn.RemoteAddr()]; ok {
		return true
	}
	return false
}

func (app *App) auth(conn redcon.Conn, auth string) bool {
	if auth == app.conf.Raptor.Auth {
		app.authed[conn.RemoteAddr()] = struct{}{}
		return true
	}

	return false
}

func (app *App) Close() error {
	return app.db.Close()
}
