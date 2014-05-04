package mysql

type Result struct {
}

func (r *Result) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *Result) RowsAffected() (int64, error) {
	return 0, nil
}
