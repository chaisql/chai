package chai

import (
	"database/sql"

	"github.com/chaisql/chai/internal/sql/driver"
)

func init() {
	sql.Register("chai", driver.Driver{})
}
