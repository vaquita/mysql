package mysql

type Tx struct {
	c *Conn
}

func (t *Tx) Commit() error {
	_, err := t.c.handleExec("COMMIT", nil)
	return err
}

func (t *Tx) Rollback() error {
	_, err := t.c.handleExec("ROLLBACK", nil)
	return err
}
