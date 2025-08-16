package dbutil

import (
	"database/sql"

	_ "github.com/chaisql/chai"
)

func OpenDB(dbPath string) (*sql.DB, error) {
	if dbPath == "" {
		dbPath = ":memory:"
	}

	return sql.Open("chai", dbPath)
}
