package shell

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/agnivade/levenshtein"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/cockroachdb/errors"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"github.com/chaisql/chai"
	"github.com/chaisql/chai/cmd/chai/dbutil"
)

const (
	historyFilename = ".chai_history"
)

var (
	// error returned when the exit command is executed
	errExitCommand = errors.New("exit command")
	// error returned when the program received a termination signal
	errExitSignal = errors.New("termination signal received")
)

// A Shell manages a command line shell program for manipulating a Chai database.
type Shell struct {
	db   *chai.DB
	opts *Options

	displayTime bool

	history []string

	// context used for execution cancellation,
	// these must not be used manually.
	// Use getExecContext and cancelExecContext instead.
	execContext  context.Context
	execCancelFn func()
	mu           sync.Mutex
}

// Options of the shell.
type Options struct {
	// Path of the database directory that will be created.
	// If empty, the database will be in-memory.
	DBPath string
}

type queryTask struct {
	q     string
	w     *bufio.Writer
	errCh chan error
}

// Run a shell.
func Run(ctx context.Context, opts *Options) error {
	if opts == nil {
		opts = new(Options)
	}

	var sh Shell

	sh.opts = opts

	db, err := dbutil.OpenDB(ctx, sh.opts.DBPath)
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

	if opts.DBPath == "" {
		fmt.Println("Opened an in-memory database.")
	} else {
		// check if the directory exists
		if _, err := os.Stat(opts.DBPath); os.IsNotExist(err) {
			fmt.Printf("Creating an on-disk database at path %s.\n", opts.DBPath)
		} else {
			fmt.Printf("Opened an on-disk database using at path %s.\n", opts.DBPath)
		}
	}

	defer func() {
		dumpErr := sh.dumpHistory()
		if dumpErr != nil {
			err = multierr.Append(err, dumpErr)
		}
	}()

	sh.history, err = sh.loadHistory()
	if err != nil {
		return err
	}

	promptExecCh := make(chan queryTask)

	// from this point, do not use the root context anymore,
	// instead use our own signal handlers.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		ui := newTUI(&sh, promptExecCh)

		p := tea.NewProgram(ui)
		_, err = p.Run()
		if err == nil {
			return errExitCommand
		}
		return err
	})

	g.Go(func() error {
		return sh.runExecutor(ctx, promptExecCh)
	})

	err = g.Wait()
	if errors.Is(err, errExitCommand) || errors.Is(err, errExitSignal) || errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}

// runExecutor manages execution. It reads user input from promptExecCh, executes any
// command or query and writes back an empty string to that channel once it's done.
func (sh *Shell) runExecutor(ctx context.Context, promptExecCh chan queryTask) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case input := <-promptExecCh:
			displayTime := sh.displayTime
			start := time.Now().UTC()
			err := sh.executeInput(sh.getExecContext(ctx), input.q, input.w)
			if errors.Is(err, context.Canceled) {
				// Print a newline for cleanliness
				fmt.Fprintln(input.w)
				input.w.Flush()
				continue
			}
			if errors.Is(err, errExitCommand) {
				input.w.Flush()
				close(input.errCh)
				return err
			}
			if err != nil {
				input.w.Flush()
				input.errCh <- err
				continue
			}

			// if showtime is true, ensure it's a query, and it was executed.
			if displayTime {
				fmt.Fprintf(input.w, "Time: %s\n", time.Since(start))
			}

			input.w.Flush()
			close(input.errCh)
		}
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
		line, err := base64.StdEncoding.DecodeString(s.Text())
		if err != nil {
			continue
		}
		history = append(history, string(line))
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

	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, h := range sh.history {
		_, err = w.WriteString(base64.StdEncoding.EncodeToString([]byte(h)) + "\n")
		if err != nil {
			return err
		}
	}

	return w.Flush()
}

func (sh *Shell) getHistoryLine(offset int) string {
	if len(sh.history) == 0 {
		return ""
	}

	offset--

	if offset >= len(sh.history) {
		return sh.history[0]
	}

	return sh.history[len(sh.history)-1-offset]
}

// executeInput stores user input in the history and executes it.
func (sh *Shell) executeInput(ctx context.Context, in string, out io.Writer) error {
	sh.history = append(sh.history, in)

	switch {
	// if it starts with a "." it's a command
	case strings.HasPrefix(in, "."):
		return sh.runCommand(ctx, in, out)
	// If it ends with a ";" we can run a query
	case strings.HasSuffix(in, ";"):
		err := sh.runQuery(ctx, in, out)
		return err
	// If the input is empty we ignore it
	case in == "":
		return nil
	}

	return nil
}

func (sh *Shell) runCommand(ctx context.Context, in string, out io.Writer) error {
	in = strings.TrimSuffix(in, ";")
	cmd := strings.Fields(in)
	switch cmd[0] {
	case ".timer":
		if len(cmd) != 2 || (cmd[1] != "on" && cmd[1] != "off") {
			return fmt.Errorf(getUsage(".timer"))
		}

		sh.displayTime = cmd[1] == "on"
		return nil
	case ".help":
		return runHelpCmd(out)
	case ".tables":
		if len(cmd) > 1 {
			return fmt.Errorf(getUsage(".tables"))
		}

		return runTablesCmd(sh.db, out)
	case ".indexes":
		if len(cmd) > 2 {
			return fmt.Errorf(getUsage(".indexes"))
		}

		var tableName string
		if len(cmd) > 1 {
			tableName = cmd[0]
		}

		return runIndexesCmd(sh.db, tableName, out)
	case ".dump":
		return dbutil.Dump(sh.db, out, cmd[1:]...)
	case ".save":
		if len(cmd) != 2 {
			return fmt.Errorf("cannot save without output path")
		}
		return runSaveCmd(ctx, sh.db, cmd[1])
	case ".schema":
		return dbutil.DumpSchema(sh.db, out, cmd[1:]...)
	case ".import":
		if len(cmd) != 4 {
			return fmt.Errorf(getUsage(".import"))
		}

		return runImportCmd(sh.db, cmd[1], cmd[2], cmd[3])
	case ".restore":
		if len(cmd) != 2 {
			return fmt.Errorf(getUsage(".restore"))
		}
		return dbutil.Restore(ctx, sh.db, cmd[1], "./")
	default:
		return displaySuggestions(in, out)
	}
}

func (sh *Shell) runQuery(ctx context.Context, q string, out io.Writer) error {
	err := dbutil.ExecSQL(ctx, sh.db, strings.NewReader(q), out)
	if errors.Is(err, context.Canceled) {
		return errors.New("interrupted")
	}

	return err
}

func shouldDisplaySuggestion(name, in string) bool {
	// input should be at least half the command size to get a suggestion.
	d := levenshtein.ComputeDistance(name, in)
	return d < (len(name) / 2)
}

// displaySuggestions shows suggestions.
func displaySuggestions(in string, out io.Writer) error {
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

	fmt.Fprintf(out, "\"%s\" is not a command. Did you mean: ", in)
	for i := range suggestions {
		if i > 0 {
			fmt.Fprintf(out, ", ")
		}

		fmt.Fprintf(out, "%q", suggestions[i])
	}

	fmt.Fprintf(out, "?")
	return nil
}
