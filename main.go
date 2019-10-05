package main

import (
	"github.com/qichengzx/raptor/config"
	"github.com/qichengzx/raptor/server"
)

func main() {
	conf, err := config.LoadConfig("config.yaml")
	if err != nil {
		panic(err)
	}

	svr := server.New(conf)
	svr.Run()
}
