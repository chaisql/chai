package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/badger"
	"github.com/asdine/genji/engine/bolt"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/record"
	"github.com/c-bata/go-prompt"
	bdg "github.com/dgraph-io/badger"
)

var suggestions = []prompt.Suggest{
	{Text: "CREATE"},
	{Text: "TABLE"},
	{Text: "SELECT"},
	{Text: "INSERT"},
	{Text: "UPDATE"},
	{Text: "DELETE"},
}

func completer(in prompt.Document) []prompt.Suggest {
	w := in.GetWordBeforeCursor()
	if w == "" {
		return []prompt.Suggest{}
	}

	return prompt.FilterHasPrefix(suggestions, w, true)
}

type executor struct {
	db    *genji.DB
	query string

	LivePrefix string
	IsEnable   bool
}

func (e *executor) Execute(in string) {
	switch in {
	case ".tables":
		err := e.TablesCmd()
		if err != nil {
			fmt.Println(err)
		}
		return
	}

	if strings.HasSuffix(in, ";") {
		e.query = e.query + in
		e.IsEnable = false
		e.LivePrefix = in
		err := e.Query(e.query)
		if err != nil {
			fmt.Println(err)
		}
		e.query = ""
		return
	}
	e.query = e.query + in + " "
	e.LivePrefix = "... "
	e.IsEnable = true
}

func (e *executor) Query(q string) error {
	res, err := e.db.Query(q)
	if err != nil {
		return err
	}

	defer res.Close()
	return record.IteratorToCSV(os.Stdout, res)
}

func (e *executor) TablesCmd() error {
	var tables []string
	err := e.db.View(func(tx *genji.Tx) error {
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

func (e *executor) ChangeLivePrefix() (string, bool) {
	return e.LivePrefix, e.IsEnable
}

func runGenjiClient(ngName, dbPath string) error {
	var ng engine.Engine
	var err error

	switch ngName {
	case "memory":
		ng = memory.NewEngine()
	case "bolt":
		ng, err = bolt.NewEngine(dbPath, 0660, nil)
	case "badger":
		opts := bdg.DefaultOptions(dbPath)
		opts.Logger = nil
		ng, err = badger.NewEngine(opts)
	}
	if err != nil {
		return err
	}

	db, err := genji.New(ng)
	if err != nil {
		return err
	}
	defer db.Close()

	switch ngName {
	case "memory":
		fmt.Println("Opened in-memory database.")
	case "bolt":
		fmt.Printf("Opened on-disk database using BoltDB engine at path %s.\n", dbPath)
	case "badger":
		fmt.Printf("Opened on-disk database using Badger engine at path %s.\n", dbPath)
	}

	e := executor{
		db: db,
	}
	p := prompt.New(
		e.Execute,
		completer,
		prompt.OptionPrefix("genji> "),
		prompt.OptionTitle("genji"),
		prompt.OptionLivePrefix(e.ChangeLivePrefix),
	)
	p.Run()
	return nil
}
