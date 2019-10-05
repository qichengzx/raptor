package raptor

func (r *Raptor) Del(key [][]byte) error {
	return r.db.Del(key)
}
