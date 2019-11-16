package shell

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/asdine/genji"
	"github.com/asdine/genji/engine"
	"github.com/asdine/genji/engine/badgerengine"
	"github.com/asdine/genji/engine/boltengine"
	"github.com/asdine/genji/engine/memoryengine"
	"github.com/asdine/genji/record"
	"github.com/c-bata/go-prompt"
	"github.com/dgraph-io/badger/v2"
)

// A Shell manages a command line shell program for manipulating a Genji database.
type Shell struct {
	db       *genji.DB
	wg       sync.WaitGroup
	opts     *Options
	cancelFn func()

	query      string
	livePrefix string
	multiLine  bool
}

// Options of the shell.
type Options struct {
	// Name of the engine to use when opening the database.
	// Must be either "memory", "bolt" or "badger"
	// If empty, "memory" will be used, unless DBPath is non empty.
	// In that case "bolt" will be used.
	Engine string
	// Path of the database file or directory that will be created.
	DBPath string
}

func (o *Options) validate() error {
	if o.Engine == "" {
		if o.DBPath == "" {
			o.Engine = "memory"
		} else {
			o.Engine = "bolt"
		}
	}

	switch o.Engine {
	case "bolt", "badger", "memory":
	default:
		return fmt.Errorf("unsupported engine %q", o.Engine)
	}

	return nil
}

// Run a shell.
func Run(opts *Options) error {
	if opts == nil {
		opts = new(Options)
	}

	err := opts.validate()
	if err != nil {
		return err
	}

	var sh Shell

	sh.opts = opts

	switch opts.Engine {
	case "memory":
		fmt.Println("Opened an in-memory database.")
	case "bolt":
		fmt.Printf("On-disk database using BoltDB engine at path %s.\n", opts.DBPath)
	case "badger":
		fmt.Printf("On-disk database using Badger engine at path %s.\n", opts.DBPath)
	}

	e := prompt.New(
		sh.execute,
		completer,
		prompt.OptionPrefix("genji> "),
		prompt.OptionTitle("genji"),
		prompt.OptionLivePrefix(sh.changelivePrefix),
	)

	e.Run()

	if sh.db != nil {
		return sh.db.Close()
	}

	return nil
}

func (sh *Shell) execute(in string) {
	err := sh.executeInput(in)
	if err != nil {
		fmt.Println(err)
	}
}

func (sh *Shell) executeInput(in string) error {
	switch {
	// if it starts with a "." it's a command
	// it must not be in the middle of a multi line query though
	case strings.HasPrefix(in, ".") && sh.query == "":
		return sh.runCommand(in)
	// If it ends with a ";" we can run a query
	case strings.HasSuffix(in, ";"):
		sh.query = sh.query + in
		sh.multiLine = false
		sh.livePrefix = in
		err := sh.runQuery(sh.query)
		sh.query = ""
		return err
	// If we reach this case, it means the user is in the middle of a
	// multi line query. We change the prompt and set the multiLine var to true.
	default:
		sh.query = sh.query + in + " "
		sh.livePrefix = "... "
		sh.multiLine = true
	}

	return nil
}

func (sh *Shell) runCommand(cmd string) error {
	switch cmd {
	case ".tables":
		db, err := sh.getDB()
		if err != nil {
			return err
		}
		return runTablesCmd(db)
	}

	return fmt.Errorf("unknown command %q", cmd)
}

func (sh *Shell) runQuery(q string) error {
	db, err := sh.getDB()
	if err != nil {
		return err
	}

	res, err := db.Query(q)
	if err != nil {
		return err
	}

	defer res.Close()
	return record.IteratorToJSON(os.Stdout, res)
}

func (sh *Shell) getDB() (*genji.DB, error) {
	if sh.db != nil {
		return sh.db, nil
	}

	var ng engine.Engine
	var err error

	switch sh.opts.Engine {
	case "memory":
		ng = memoryengine.NewEngine()
	case "bolt":
		ng, err = boltengine.NewEngine(sh.opts.DBPath, 0660, nil)
	case "badger":
		opts := badger.DefaultOptions(sh.opts.DBPath)
		opts.Logger = nil
		ng, err = badgerengine.NewEngine(opts)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	sh.db, err = genji.New(ng)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	return sh.db, nil
}

func (sh *Shell) changelivePrefix() (string, bool) {
	return sh.livePrefix, sh.multiLine
}

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
