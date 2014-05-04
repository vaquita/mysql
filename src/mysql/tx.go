package mysql

type Tx struct {
}

func (t *Tx) Commit() error {
	return nil
}

func (t *Tx) Rollback() error {
	return nil
}
