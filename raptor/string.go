package raptor

func (r *Raptor) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	return r.db.Get(key)
}

func (r *Raptor) Set(key, value []byte) error {
	if len(key) == 0 || len(value) == 0 {
		return ErrParams
	}
	return r.db.Set(key, value)
}

func (r *Raptor) Strlen(key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, ErrKeyEmpty
	}

	return r.db.Strlen(key)
}
