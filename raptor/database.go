package raptor

//TODO check params

func (r *Raptor) Del(key [][]byte) error {
	return r.db.Del(key)
}

func (r *Raptor) Exists(key []byte) error {
	return r.db.Exists(key)
}
