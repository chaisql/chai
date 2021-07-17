package shell

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/agnivade/levenshtein"
	"github.com/c-bata/go-prompt"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/cmd/genji/dbutil"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/sql/parser"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

const (
	historyFilename = ".genji_history"
)

var (
	// error returned when the exit command is executed
	errExitCommand = errors.New("exit command")
	// error returned when the program received a termination signal
	errExitSignal = errors.New("termination signal received")
	// error returned when the prompt reads a ctrl d input
	errExitCtrlD = errors.New("ctrl-d")
)

// A Shell manages a command line shell program for manipulating a Genji database.
type Shell struct {
	db   *genji.DB
	opts *Options

	query      string
	livePrefix string
	multiLine  bool

	history []string

	cmdSuggestions []prompt.Suggest

	// context used for execution cancellation,
	// these must not be used manually.
	// Use getExecContext and cancelExecContext instead.
	execContext  context.Context
	execCancelFn func()
	mu           sync.Mutex
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

	// Badger only:
	EncryptionKey string
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
func Run(ctx context.Context, opts *Options) error {
	if opts == nil {
		opts = new(Options)
	}

	err := opts.validate()
	if err != nil {
		return err
	}

	var sh Shell

	sh.opts = opts

	db, err := dbutil.OpenDB(ctx, sh.opts.DBPath, sh.opts.Engine, dbutil.DBOptions{EncryptionKey: opts.EncryptionKey})
	if err != nil {
		return err
	}
	sh.db = db.WithContext(ctx)
	defer func() {
		closeErr := sh.db.Close()
		if closeErr != nil {
			err = multierr.Append(err, closeErr)
		}
	}()

	switch opts.Engine {
	case "memory":
		fmt.Println("Opened an in-memory database.")
	case "bolt":
		fmt.Printf("On-disk database using BoltDB engine at path %s.\n", opts.DBPath)
	case "badger":
		fmt.Printf("On-disk database using Badger engine at path %s.\n", opts.DBPath)
	}
	fmt.Println("Enter \".help\" for usage hints.")

	defer func() {
		dumpErr := sh.dumpHistory()
		if dumpErr != nil {
			err = multierr.Append(err, dumpErr)
		}
	}()

	promptExecCh := make(chan string)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return sh.runSignalHandlers(ctx)
	})

	g.Go(func() error {
		return sh.runExecutor(ctx, promptExecCh)
	})

	// Because go-prompt doesn't handle cancellation
	// it is impossible to ask it to stop when the prompt.Input function
	// is running.
	// We run it in a non-managed goroutine with no graceful shutdown
	// so that if the prompt.Input function is running when we want to quit the program
	// we simply don't wait for this goroutine to end.
	// This goroutine must not manage any resource.
	go func() {
		defer cancel()

		err := sh.runPrompt(ctx, promptExecCh)
		if err != nil && err != errExitCtrlD {
			fmt.Fprintln(os.Stderr, err.Error())
		}
	}()

	err = g.Wait()
	if err == errExitCommand || err == errExitSignal || err == context.Canceled {
		return nil
	}

	return err
}

// runSignalHandlers handles two different signals.
// On SIGINT, it cancels any query execution using sh.cancelExecution.
// On SIGTERM, it triggers graceful shutdown by returning errExitSignal.
func (sh *Shell) runSignalHandlers(ctx context.Context) error {
	interruptC := make(chan os.Signal, 1)
	termC := make(chan os.Signal, 1)
	signal.Notify(interruptC, os.Interrupt)
	signal.Notify(termC, syscall.SIGTERM)

	for {
		select {
		case <-interruptC:
			sh.cancelExecution()
		case <-termC:
			fmt.Fprintf(os.Stderr, "\nTermination signal received. Quitting...\n")
			return errExitSignal
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// runExecutor manages execution. It reads user input from promptExecCh, executes any
// command or query and writes back an empty string to that channel once it's done.
func (sh *Shell) runExecutor(ctx context.Context, promptExecCh chan string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case input := <-promptExecCh:
			err := sh.executeInput(sh.getExecContext(ctx), input)
			// if the context has been canceled
			// there is no way to tell at this point
			// if this is because of a user interruption
			// or a termination signal.
			// If it's the latter, it will be detected by the Select statement.
			if err == context.Canceled {
				// Print a newline for cleanliness
				fmt.Println()
				continue
			}
			if err == errExitCommand {
				return err
			}
			if err != nil {
				fmt.Println(err)
			}
		case promptExecCh <- "":
		}
	}
}

// runPrompt is a stateless function that displays a prompt to the user.
// User input is sent to the execCh channel which must deal with parsing and error handling.
// Once the execution of the user input is done by the reader of the channel, it must
// send a string back to execCh so that this function will display another prompt.
func (sh *Shell) runPrompt(ctx context.Context, execCh chan (string)) error {
	sh.loadCommandSuggestions()
	history, err := sh.loadHistory()
	if err != nil {
		return err
	}

	// we store the last key stroke to
	// determine if ctrl D was pressed by the user.
	var lastKeyStroke prompt.Key

	promptOpts := []prompt.Option{
		prompt.OptionPrefix("genji> "),
		prompt.OptionTitle("Genji"),
		prompt.OptionLivePrefix(sh.changelivePrefix),
		prompt.OptionHistory(history),
		prompt.OptionBreakLineCallback(func(d *prompt.Document) {
			lastKeyStroke = d.LastKeyStroke()
		}),
	}

	// If NO_COLOR env var is present, disable color. See https://no-color.org
	if _, ok := os.LookupEnv("NO_COLOR"); ok {
		// A list of color options we have to reset.
		colorOpts := []func(prompt.Color) prompt.Option{
			prompt.OptionPrefixTextColor,
			prompt.OptionPreviewSuggestionTextColor,
			prompt.OptionSuggestionTextColor,
			prompt.OptionSuggestionBGColor,
			prompt.OptionSelectedSuggestionTextColor,
			prompt.OptionSelectedSuggestionBGColor,
			prompt.OptionDescriptionTextColor,
			prompt.OptionDescriptionBGColor,
			prompt.OptionSelectedDescriptionTextColor,
			prompt.OptionSelectedDescriptionBGColor,
			prompt.OptionScrollbarThumbColor,
			prompt.OptionScrollbarBGColor,
		}
		for _, opt := range colorOpts {
			resetColor := opt(prompt.DefaultColor)
			promptOpts = append(promptOpts, resetColor)
		}
	}

	pt := prompt.New(
		func(in string) {},
		sh.completer,
		promptOpts...,
	)

	for {
		// Input() captures ctrl D and ctrl C.
		// It never returns when ctrl C is pressed but does on CTRL D
		// under specific conditions.
		input := pt.Input()

		// go-prompt ignores ctrl D if it was pressed while the line is not empty.
		// However, it returns if the line is empty and sets lastKeyStroke to prompt.ControlD.
		// if so, we must stop the program.
		if lastKeyStroke == prompt.ControlD {
			return errExitCtrlD
		}

		input = strings.TrimSpace(input)

		if len(input) == 0 {
			continue
		}

		// delegate execution to the sh.runExecutor goroutine
		execCh <- input
		// and wait for it to finish to display another prompt.
		<-execCh
	}
}

// cancelExecution must be called to cancel any ongoing execution without
// stopping the program.
// Calling this function when there is no ongoing execution is a no-op.
func (sh *Shell) cancelExecution() {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.execCancelFn != nil {
		sh.execCancelFn()
		sh.execContext = nil
		sh.execCancelFn = nil
	}
}

// getExecContext returns the current cancelable execution context
// or creates one if needed.
func (sh *Shell) getExecContext(ctx context.Context) context.Context {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.execContext != nil {
		return sh.execContext
	}

	sh.execContext, sh.execCancelFn = context.WithCancel(ctx)
	return sh.execContext
}

func (sh *Shell) loadCommandSuggestions() {
	suggestions := make([]prompt.Suggest, 0, len(commands))
	for _, c := range commands {
		suggestions = append(suggestions, prompt.Suggest{
			Text: c.Name,
		})

		for _, alias := range c.Aliases {
			suggestions = append(suggestions, prompt.Suggest{
				Text: alias,
			})
		}
	}
	sh.cmdSuggestions = suggestions
}

func (sh *Shell) loadHistory() ([]string, error) {
	if _, ok := os.LookupEnv("NO_HISTORY"); ok {
		return nil, nil
	}
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
	if _, ok := os.LookupEnv("NO_HISTORY"); ok {
		return nil
	}
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

// executeInput stores user input in the history and executes it.
func (sh *Shell) executeInput(ctx context.Context, in string) error {
	sh.history = append(sh.history, in)

	switch {
	// if it starts with a "." it's a command
	// if the input is "help" or "exit", then it's a command.
	// it must not be in the middle of a multi line query though
	case !sh.multiLine && strings.HasPrefix(in, "."), in == "help", in == "exit":
		return sh.runCommand(ctx, in)
	// If it ends with a ";" we can run a query
	case strings.HasSuffix(in, ";"):
		sh.query = sh.query + in
		sh.multiLine = false
		sh.livePrefix = in
		err := sh.runQuery(ctx, sh.query)
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

func (sh *Shell) runCommand(ctx context.Context, in string) error {
	in = strings.TrimSuffix(in, ";")
	cmd := strings.Fields(in)
	switch cmd[0] {
	case ".help", "help":
		return runHelpCmd()
	case ".tables":
		if len(cmd) > 1 {
			return fmt.Errorf(getUsage(".tables"))
		}

		return runTablesCmd(sh.db, os.Stdout)
	case ".exit", "exit":
		if len(cmd) > 1 {
			return fmt.Errorf(getUsage(".exit"))
		}

		return errExitCommand
	case ".indexes":
		if len(cmd) > 2 {
			return fmt.Errorf(getUsage(".indexes"))
		}

		var tableName string
		if len(cmd) > 1 {
			tableName = cmd[0]
		}

		return runIndexesCmd(sh.db, tableName, os.Stdout)
	case ".dump":
		return dbutil.Dump(ctx, sh.db, os.Stdout, cmd[1:]...)
	case ".save":
		var engine, path string
		if len(cmd) > 2 {
			engine = cmd[1]
			path = cmd[2]
		} else if len(cmd) == 2 {
			engine = "bolt"
			path = cmd[1]
		} else {
			return fmt.Errorf("can't save without output path")
		}

		return runSaveCmd(ctx, sh.db, engine, path)
	case ".schema":
		return dbutil.DumpSchema(ctx, sh.db, os.Stdout, cmd[1:]...)
	case ".import":
		if len(cmd) != 4 {
			return fmt.Errorf(getUsage(".import"))
		}

		return runImportCmd(ctx, sh.db, cmd[1], cmd[2], cmd[3])
	case ".doc":
		if len(cmd) != 2 {
			return fmt.Errorf(getUsage(".doc"))
		}
		return runDocCmd(cmd[1])
	default:
		return displaySuggestions(in)
	}
}

func (sh *Shell) runQuery(ctx context.Context, q string) error {
	err := dbutil.ExecSQL(ctx, sh.db, strings.NewReader(q), os.Stdout)
	if err == context.Canceled {
		return errors.New("interrupted")
	}

	return err
}

func (sh *Shell) changelivePrefix() (string, bool) {
	return sh.livePrefix, sh.multiLine
}

// getTables returns all the tables of the database
func (sh *Shell) getAllTables(ctx context.Context) ([]string, error) {
	var tables []string

	res, err := sh.db.Query("SELECT name FROM __genji_catalog WHERE type = 'table'")
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
	if err != nil {
		return nil, err
	}

	// if there is no table return table as a suggestion
	if len(tables) == 0 {
		tables = append(tables, "table_name")
	}

	return tables, nil
}

func (sh *Shell) completer(in prompt.Document) []prompt.Suggest {
	suggestions := prompt.FilterHasPrefix(sh.cmdSuggestions, in.Text, true)

	_, err := parser.ParseQuery(in.Text)
	if err != nil {
		e, ok := err.(*parser.ParseError)
		if !ok || len(e.Expected) < 1 {
			return suggestions
		}
		expected := e.Expected
		switch expected[0] {
		case "table_name":
			expected, err = sh.getAllTables(context.Background())
			if err != nil {
				return suggestions
			}
		case "index_name":
			expected, err = dbutil.ListIndexes(context.Background(), sh.db, "")
			if err != nil {
				return suggestions
			}
		}
		for _, e := range expected {
			suggestions = append(suggestions, prompt.Suggest{
				Text: e,
			})
		}

		w := in.GetWordBeforeCursor()
		if w == "" {
			return suggestions
		}

		return prompt.FilterHasPrefix(suggestions, w, true)
	}

	return []prompt.Suggest{}
}

func shouldDisplaySuggestion(name, in string) bool {
	// input should be at least half the command size to get a suggestion.
	d := levenshtein.ComputeDistance(name, in)
	return d < (len(name) / 2)
}

// displaySuggestions shows suggestions.
func displaySuggestions(in string) error {
	var suggestions []string
	for _, c := range commands {
		if shouldDisplaySuggestion(c.Name, in) {
			suggestions = append(suggestions, c.Name)
		}

		for _, alias := range c.Aliases {
			if shouldDisplaySuggestion(alias, in) {
				suggestions = append(suggestions, alias)
			}
		}
	}

	if len(suggestions) == 0 {
		return fmt.Errorf("Unknown command %q. Enter \".help\" for help.", in)
	}

	fmt.Printf("\"%s\" is not a command. Did you mean: ", in)
	for i := range suggestions {
		if i > 0 {
			fmt.Printf(", ")
		}

		fmt.Printf("%q", suggestions[i])
	}

	fmt.Println()
	return nil
}
