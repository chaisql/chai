package genji

import (
	"database/sql"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/query/driver"
)

func Open(ng engine.Engine) (*sql.DB, error) {
	db, err := database.New(ng)
	if err != nil {
		return nil, err
	}

	return sql.OpenDB(driver.NewConnector(db)), nil
}
