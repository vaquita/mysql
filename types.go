package mysql

type nullString struct {
	value string
	valid bool // valid is true if 'the string' is not NULL
}