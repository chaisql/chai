package shell

import (
	"fmt"

	"github.com/genjidb/genji"
)

func runTablesCmd(db *genji.DB) error {
	var tables []string
	err := db.View(func(tx *genji.Tx) error {
		tables = tx.ListTables()
		return nil
	})
	if err != nil {
		return err
	}

	for _, t := range tables {
		fmt.Println(t)
	}

	return nil
}
