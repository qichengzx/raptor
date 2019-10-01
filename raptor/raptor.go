package raptor

import (
	"github.com/qichengzx/raptor/config"
	"github.com/qichengzx/raptor/storage"
)

type Raptor struct {
	db storage.DB
}

func New(conf *config.Config) (*Raptor, error) {
	db, err := storage.Open(conf)
	if err != nil {
		return nil, err
	}

	return &Raptor{db: db}, nil
}

func (r *Raptor) Close() error {
	return r.db.Close()
}
