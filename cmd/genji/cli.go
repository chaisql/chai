package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine/memory"
	"github.com/asdine/genji/record"
	"github.com/c-bata/go-prompt"
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

func (e *executor) ChangeLivePrefix() (string, bool) {
	return e.LivePrefix, e.IsEnable
}

func runGenjiClient() error {
	db, err := genji.New(memory.NewEngine())
	if err != nil {
		return err
	}
	defer db.Close()

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
