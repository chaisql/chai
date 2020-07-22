package shell

import (
	"fmt"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
)

func runTablesCmd(db *genji.DB, cmd []string) error {
	if len(cmd) > 1 {
		return fmt.Errorf("too many arguments in call to %s", cmd[0])
	}

	var tables []string
	err := db.View(func(tx *genji.Tx) error {
		var err error

		tables, err = tx.ListTables()
		return err
	})
	if err != nil {
		return err
	}

	for _, t := range tables {
		fmt.Println(t)
	}

	return nil
}

// runTableIndexCmd shows the all indexes that the given table contains.
func runTableIndexCmd(db *genji.DB, tableName string) error {
	err := db.ViewTable(tableName, func(tx *genji.Tx, table *database.Table) error {
		return table.PrintIndexes()
	})

	if err == document.ErrFieldNotFound {
		return nil
	}

	return err
}

// runAllIndexesCmd shows all indexes that the database contains.
func runAllIndexesCmd(db *genji.DB) error {
	var tables []string
	err := db.View(func(tx *genji.Tx) error {
		var err error
		tables, err = tx.ListTables()
		return err
	})

	if err != nil {
		return err
	}

	for _, table := range tables {
		// If there is no index in a table we continue to the next. No error handling needed
		_ = db.View(func(tx *genji.Tx) error {
			t, err := tx.GetTable(table)
			if err != nil {
				return err
			}

			return t.PrintIndexes()
		})
	}

	return nil
}

// runIndexesCmd select a kind of indexes command is wanted
func runIndexesCmd(db *genji.DB, in []string) error {
	switch len(in) {
	case 1:
		// If the input is ".Indexes"
		return runAllIndexesCmd(db)
	case 2:
		// If the input is ".Indexes <tableName>" cmd[1] is the table name
		return runTableIndexCmd(db, in[1])
	}

	return fmt.Errorf("too many arguments in call to %s", in[0])
}
