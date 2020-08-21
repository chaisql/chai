package shell

import (
	"fmt"
	"sort"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
)

var commands = map[string]string{
	".tables":               "\t\tList names of tables.",
	".exit":                 "\t\t\tExit this program.",
	".indexes [table_name]": "\tDisplay all indexes or the indexes of the given table name.",
	".help":                 "\t\t\tList all commands.",
}

func runTablesCmd(db *genji.DB, cmd []string) error {
	if len(cmd) > 1 {
		return fmt.Errorf("usage: .tables")
	}

	res, err := db.Query("SELECT table_name FROM __genji_tables")
	if err != nil {
		return err
	}
	defer res.Close()

	return res.Iterate(func(d document.Document) error {
		var tableName string
		err = document.Scan(d, &tableName)
		if err != nil {
			return err
		}
		fmt.Println(tableName)
		return nil
	})
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

// runHelpCmd display all available dot commands.
func runHelpCmd() error {
	var keys []string
	for k := range commands {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%s %s\n", k, commands[k])
	}

	return nil
}
