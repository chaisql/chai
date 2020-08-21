package shell

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/c-bata/go-prompt"
	"github.com/dgraph-io/badger/v2"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/engine/boltengine"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/genjidb/genji/sql/parser"
)

const (
	historyFilename = ".genji_history"
)

// A Shell manages a command line shell program for manipulating a Genji database.
type Shell struct {
	db   *genji.DB
	opts *Options

	query      string
	livePrefix string
	multiLine  bool

	history []string
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

	fmt.Println("Enter \".help\" for usage hints.")
	history, err := sh.loadHistory()
	if err != nil {
		return err
	}

	ran, err := sh.runPipedInput()
	if err != nil {
		return err
	}
	if ran {
		return nil
	}

	e := prompt.New(
		sh.execute,
		sh.completer,
		prompt.OptionPrefix("genji> "),
		prompt.OptionTitle("genji"),
		prompt.OptionLivePrefix(sh.changelivePrefix),
		prompt.OptionHistory(history),
	)

	e.Run()

	if sh.db != nil {
		err = sh.db.Close()
		if err != nil {
			return err
		}
	}

	return sh.dumpHistory()
}

func (sh *Shell) loadHistory() ([]string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	fname := filepath.Join(homeDir, historyFilename)

	_, err = os.Stat(fname)
	if err != nil {
		return nil, nil
	}

	f, err := os.Open(fname)
	if err != nil {
		return nil, nil
	}
	defer f.Close()

	var history []string
	s := bufio.NewScanner(f)
	for s.Scan() {
		history = append(history, s.Text())
	}

	return history, s.Err()
}

func (sh *Shell) dumpHistory() error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	fname := filepath.Join(homeDir, historyFilename)

	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, h := range sh.history {
		_, err = w.WriteString(h + "\n")
		if err != nil {
			return err
		}
	}

	return w.Flush()
}

func (sh *Shell) execute(in string) {
	sh.history = append(sh.history, in)

	err := sh.executeInput(in)
	if err != nil {
		fmt.Println(err)
	}
}

func (sh *Shell) executeInput(in string) error {
	in = strings.TrimSpace(in)
	switch {
	// if it starts with a "." it's a command
	// it must not be in the middle of a multi line query though
	case strings.HasPrefix(in, "."):
		return sh.runCommand(in)
	// If it ends with a ";" we can run a query
	case strings.HasSuffix(in, ";"):
		sh.query = sh.query + in
		sh.multiLine = false
		sh.livePrefix = in
		err := sh.runQuery(sh.query)
		sh.query = ""
		return err
	// If the input is empty we ignore it
	case in == "":
		return nil

	// If we reach this case, it means the user is in the middle of a
	// multi line query. We change the prompt and set the multiLine var to true.
	default:
		sh.query = sh.query + in + " "
		sh.livePrefix = "... "
		sh.multiLine = true
	}

	return nil
}

func (sh *Shell) runCommand(in string) error {
	cmd := strings.Fields(in)
	switch cmd[0] {
	case ".tables":
		db, err := sh.getDB()
		if err != nil {
			return err
		}

		return runTablesCmd(db, cmd)
	case ".exit":
		if len(cmd) > 1 {
			return fmt.Errorf("usage: .exit")
		}

		os.Exit(0)
	case ".indexes":
		db, err := sh.getDB()
		if err != nil {
			return err
		}

		return runIndexesCmd(db, cmd)
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
	return document.IteratorToJSON(os.Stdout, res)
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
		ng, err = badgerengine.NewEngine(badger.DefaultOptions(sh.opts.DBPath).WithLogger(nil))
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

func (sh *Shell) runPipedInput() (ran bool, err error) {
	// Check if there is any input being piped in from the terminal
	stat, _ := os.Stdin.Stat()
	m := stat.Mode()

	if (m&os.ModeNamedPipe) == 0 /*cat a.txt| prog*/ && !m.IsRegular() /*prog < a.txt*/ { // No input from terminal
		return false, nil
	}
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return true, fmt.Errorf("Unable to read piped input: %w", err)
	}
	err = sh.runQuery(string(data))
	if err != nil {
		return true, fmt.Errorf("Unable to execute provided sql statements: %w", err)
	}

	return true, nil
}

func (sh *Shell) changelivePrefix() (string, bool) {
	return sh.livePrefix, sh.multiLine
}

// getTables returns all the tables of the database
func (sh *Shell) getAllTables() ([]string, error) {
	var tables []string
	db, _ := sh.getDB()
	res, err := db.Query("SELECT table_name FROM __genji_tables")
	if err != nil {
		return nil, err
	}

	defer res.Close()

	err = res.Iterate(func(d document.Document) error {
		var tableName string
		err = document.Scan(d, &tableName)
		if err != nil {
			return err
		}
		tables = append(tables, tableName)
		return nil
	})

	// if there is no table return table as a suggestion
	if len(tables) == 0 {
		tables = append(tables, "table_name")
	}

	return tables, nil
}

func (sh *Shell) completer(in prompt.Document) []prompt.Suggest {
	_, err := parser.NewParser(strings.NewReader(in.Text)).ParseQuery()
	if err != nil {
		e, ok := err.(*parser.ParseError)
		if !ok {
			return []prompt.Suggest{}
		}
		expected := e.Expected
		switch expected[0] {
		case "table_name":
			expected, err = sh.getAllTables()
			if err != nil {
				return []prompt.Suggest{}
			}
		}

		suggestions := make([]prompt.Suggest, len(expected))
		for i, e := range expected {
			suggestions[i].Text = e
		}

		w := in.GetWordBeforeCursor()
		if w == "" {
			return suggestions
		}

		return prompt.FilterHasPrefix(suggestions, w, true)
	}

	return []prompt.Suggest{}
}
