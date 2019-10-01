package storage

import (
	"github.com/qichengzx/raptor/config"
	"github.com/qichengzx/raptor/storage/badger"
)

func Open(conf *config.Config) (DB, error) {
	return badger.Open(conf)
}
