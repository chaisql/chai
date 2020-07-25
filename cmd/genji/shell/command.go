package shell

import (
	"fmt"

	"github.com/genjidb/genji"
)

func runTablesCmd(db *genji.DB, cmd []string) error {
	if len(cmd) > 1 {
		return fmt.Errorf("usage: .tables")
	}

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

// displayTableIndex prints all indexes that the given table contains.
func displayTableIndex(db *genji.DB, tableName string) error {
	err := db.View(func(tx *genji.Tx) error {
		t, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		indexes, err := t.Indexes()
		if err != nil {
			return err
		}

		for _, idx := range indexes {
			fmt.Printf("%s on %s(%s)\n", idx.Opts.IndexName, idx.Opts.TableName, idx.Opts.Path)
		}

		return nil
	})

	return err
}

// displayAllIndexes shows all indexes that the database contains.
func displayAllIndexes(db *genji.DB) error {
	err := db.View(func(tx *genji.Tx) error {
		indexes, err := tx.ListIndexes()
		if err != nil {
			return err
		}

		for _, idx := range indexes {
			fmt.Printf("%s on %s(%s)\n", idx.IndexName, idx.TableName, idx.Path)
		}

		return nil
	})

	return err
}

// runIndexesCmd executes all indexes of the database or all indexes of the given table.
func runIndexesCmd(db *genji.DB, in []string) error {
	switch len(in) {
	case 1:
		// If the input is ".indexes"
		return displayAllIndexes(db)
	case 2:
		// If the input is ".indexes <tableName>"
		return displayTableIndex(db, in[1])

	}

	return fmt.Errorf("usage: .indexes [tablename]")
}
