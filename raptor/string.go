package raptor

func (r *Raptor) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	return r.db.Get(key)
}

func (r *Raptor) GetSet(key, value []byte) ([]byte, error) {
	if len(key) == 0 || len(value) == 0 {
		return nil, ErrParams
	}
	return r.db.GetSet(key, value)
}

func (r *Raptor) Set(key, value []byte) error {
	if len(key) == 0 || len(value) == 0 {
		return ErrParams
	}
	return r.db.Set(key, value)
}

func (r *Raptor) SetNX(key, value []byte) (bool, error) {
	if len(key) == 0 || len(value) == 0 {
		return false, ErrParams
	}
	return r.db.SetNX(key, value)
}

func (r *Raptor) Strlen(key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, ErrKeyEmpty
	}

	return r.db.Strlen(key)
}

func (r *Raptor) Incr(key []byte) (int64, error) {
	return r.db.Incr(key)
}

func (r *Raptor) IncrBy(key []byte, by int64) (int64, error) {
	return r.db.IncrBy(key, by)
}

func (r *Raptor) Decr(key []byte) (int64, error) {
	return r.db.Decr(key)
}

func (r *Raptor) DecrBy(key []byte, by int64) (int64, error) {
	return r.db.DecrBy(key, by)
}

func (r *Raptor) MSet(keys, values [][]byte) error {
	return r.db.MSet(keys, values)
}
