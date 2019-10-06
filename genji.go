package genji

import (
	"database/sql"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/engine"
)

func Open(ng engine.Engine) (*sql.DB, error) {
	db, err := database.New(ng)
	if err != nil {
		return nil, err
	}

	return sql.OpenDB(newConnector(db)), nil
}
