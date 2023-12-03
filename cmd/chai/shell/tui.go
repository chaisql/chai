package shell

import (
	"bufio"
	"bytes"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/cursor"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type model struct {
	shell        *Shell
	input        queryInputModel
	runner       queryRunnerModel
	runningQuery bool
}

func newTUI(shell *Shell, qch chan queryTask) model {
	return model{
		shell:  shell,
		input:  newQueryInputModel(shell),
		runner: newQueryRunModel(qch),
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.input.Init(),
		m.runner.Init(),
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			if m.runningQuery {
				m.shell.cancelExecution()
			}
		}
	case runCmdMsg:
		m.runningQuery = true
	case doneMsg:
		m.runningQuery = false
	}

	var runnerCmd, inputCmd tea.Cmd
	m.runner, runnerCmd = m.runner.Update(msg)
	m.input, inputCmd = m.input.Update(msg)
	return m, tea.Batch(runnerCmd, inputCmd)
}

func (m model) View() string {
	if !m.runningQuery {
		return m.input.View() + "\n"
	}

	return m.runner.View() + "\n"
}

type queryInputModel struct {
	debug         bool
	shell         *Shell
	textArea      textarea.Model
	disabled      bool
	err           error
	historyOffset int
	currentQuery  *string
}

func newQueryInputModel(shell *Shell) queryInputModel {
	ta := textarea.New()
	ta.Prompt = "... "
	ta.Placeholder = ""
	ta.ShowLineNumbers = false
	ta.FocusedStyle.Prompt = lipgloss.NewStyle()
	ta.FocusedStyle.CursorLine = ta.FocusedStyle.CursorLine.UnsetBackground()
	ta.FocusedStyle.Text = ta.FocusedStyle.Text.UnsetBackground()
	ta.Cursor.SetMode(cursor.CursorStatic)
	ta.MaxWidth = 0
	ta.SetHeight(1)
	ta.SetPromptFunc(6, func(lineIdx int) string {
		if lineIdx == 0 {
			return "chai> "
		}

		return "... "
	})
	ta.Focus()

	return queryInputModel{
		textArea: ta,
		shell:    shell,
	}
}

func (m queryInputModel) Init() tea.Cmd {
	return nil
}

func (m queryInputModel) Update(msg tea.Msg) (queryInputModel, tea.Cmd) {
	var cmd tea.Cmd

	if m.disabled {
		switch msg := msg.(type) {
		case doneMsg:
			m.disabled = false
			return m, m.textArea.Focus()
		case errorMsg:
			m.err = msg.err
			return m, nil
		default:
			m.textArea, cmd = m.textArea.Update(msg)
			return m, cmd
		}
	}

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.textArea.SetWidth(msg.Width - 1)
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			freeze := m.freezeAndReset()
			return m, tea.Println(freeze)
		case tea.KeyCtrlD:
			if m.textArea.Value() == "" {
				return m, tea.Sequence(m.textArea.Cursor.SetMode(cursor.CursorHide), tea.Quit)
			}
		case tea.KeyUp:
			if m.textArea.Line() == 0 {
				if m.historyOffset == 0 {
					v := m.textArea.Value()
					m.currentQuery = &v
				}
				if m.historyOffset < len(m.shell.history) {
					m.historyOffset++
				}
				line := m.shell.getHistoryLine(m.historyOffset)
				m.textArea.SetValue(line)
				m.textArea.SetHeight(m.textArea.LineCount())
				for m.textArea.Line() > 0 {
					m.textArea.CursorUp()
				}
			}
		case tea.KeyDown:
			if m.textArea.Line() == m.textArea.Height()-1 {
				if m.historyOffset > 0 {
					m.historyOffset--
				}
				if m.historyOffset == 0 {
					if m.currentQuery != nil {
						m.textArea.SetValue(*m.currentQuery)
						m.textArea.SetHeight(m.textArea.LineCount())
					}
				} else {
					line := m.shell.getHistoryLine(m.historyOffset)
					m.textArea.SetValue(line)
					m.textArea.SetHeight(m.textArea.LineCount())
				}
				for m.textArea.Line() < m.textArea.LineCount()-1 {
					m.textArea.CursorDown()
				}

			}
		case tea.KeyEnter:
			m.historyOffset = 0
			m.currentQuery = nil
			query := m.textArea.Value()
			clean := strings.TrimSpace(query)
			if clean == "" {
				freeze := m.freezeAndReset()
				return m, tea.Println(freeze)
			}

			if clean == "exit" || clean == ".exit" {
				m.textArea.Blur()
				m.textArea, cmd = m.textArea.Update(msg)
				return m, tea.Sequence(
					cmd,
					tea.Quit,
				)
			}

			shouldRun := m.textArea.LineCount() == 1 && strings.HasPrefix(clean, ".")
			if !shouldRun {
				shouldRun = strings.HasSuffix(strings.TrimSpace(query), ";")
			}

			if shouldRun {
				freeze := m.freezeAndReset()
				m.disabled = true
				m.textArea.Blur()
				return m, tea.Sequence(
					tea.Println(freeze),
					runCmd(query),
				)
			}

			if m.textArea.LineCount() == m.textArea.Height() {
				m.textArea.SetHeight(m.textArea.Height() + 1)
			}
		}
	default:
		if m.textArea.LineCount() < m.textArea.Height() {
			m.textArea.SetHeight(m.textArea.LineCount())
		}
	}

	m.textArea, cmd = m.textArea.Update(msg)

	return m, cmd
}

func (m queryInputModel) View() string {
	if m.err != nil {
		return "Error: " + m.err.Error() + "\n" + m.textArea.View() + "\n"
	}
	if !m.debug {
		return m.textArea.View() + "\n"
	}

	return m.textArea.View() + "\n" +
		"Width: " + strconv.Itoa(m.textArea.Width()) + "\n" +
		"Height: " + strconv.Itoa(m.textArea.Height()) + "\n" +
		"Line: " + strconv.Itoa(m.textArea.Line()) + "\n" +
		"LineCount: " + strconv.Itoa(m.textArea.LineCount()) + "\n" +
		"Len: " + strconv.Itoa(m.textArea.Length()) + "\n" +
		"Line W: " + strconv.Itoa(m.textArea.LineInfo().Width) + "\n" +
		"Line H: " + strconv.Itoa(m.textArea.LineInfo().Height) + "\n" +
		"Line CharOffset: " + strconv.Itoa(m.textArea.LineInfo().CharOffset) + "\n" +
		"Line CharWidth: " + strconv.Itoa(m.textArea.LineInfo().CharWidth) + "\n" +
		"Line StartColumn: " + strconv.Itoa(m.textArea.LineInfo().StartColumn) + "\n" +
		"Line ColumnOffset: " + strconv.Itoa(m.textArea.LineInfo().ColumnOffset) + "\n" +
		"Line RowOffset: " + strconv.Itoa(m.textArea.LineInfo().RowOffset) + "\n" +
		"HistoryOffset: " + strconv.Itoa(m.historyOffset) + "\n"
}

func (m *queryInputModel) freezeAndReset() string {
	m.textArea.Cursor.SetMode(cursor.CursorHide)
	freeze := strings.TrimSuffix(m.View(), "\n")
	m.textArea.SetValue("")
	m.textArea.SetHeight(1)
	m.textArea.Cursor.SetMode(cursor.CursorStatic)
	m.err = nil

	return freeze
}

type queryRunnerModel struct {
	spinner spinner.Model
	ch      chan string
	qch     chan queryTask
	done    chan struct{}
}

func newQueryRunModel(qch chan queryTask) queryRunnerModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	return queryRunnerModel{
		spinner: s,
		qch:     qch,
	}
}

func (m queryRunnerModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m queryRunnerModel) Update(msg tea.Msg) (queryRunnerModel, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case runCmdMsg:
		m, cmd = m.runCmd(string(msg))
		return m, tea.Batch(
			cmd,
			m.processCmd(),
		)
	case resultMsg:
		return m, tea.Batch(
			tea.Println(strings.TrimSuffix(string(msg), "\n")),
			m.processCmd(),
		)
	case errorMsg:
		return m, m.processCmd()
	case doneMsg:
		if m.ch == nil {
			return m, nil
		}

		close(m.ch)
		m.ch = nil
		m.done = nil
		return m, nil
	}

	m.spinner, cmd = m.spinner.Update(msg)

	return m, cmd
}

func (m queryRunnerModel) View() string {
	return m.spinner.View() + "\n"
}

func runCmd(q string) tea.Cmd {
	return func() tea.Msg {
		return runCmdMsg(q)
	}
}

type runCmdMsg string
type resultMsg string
type doneMsg struct{}
type errorMsg struct {
	err error
}

type teaWriter struct {
	ch  chan string
	buf bytes.Buffer
}

func (w *teaWriter) Write(p []byte) (n int, err error) {
	idx := bytes.LastIndex(p, []byte("\n"))

	if idx == len(p)-1 {
		w.buf.Write(p)
		w.ch <- w.buf.String()
		w.buf.Reset()
	} else {
		w.buf.Write(p[:idx])
		w.ch <- string(p[:idx+1])
		w.buf.Reset()
		w.buf.Write(p[idx+1:])
	}

	return len(p), nil
}

func (m queryRunnerModel) runCmd(q string) (queryRunnerModel, tea.Cmd) {
	m.ch = make(chan string)
	w := teaWriter{ch: m.ch}
	m.done = make(chan struct{})

	return m, func() tea.Msg {
		defer close(m.done)

		task := queryTask{
			q:     q,
			w:     bufio.NewWriter(&w),
			errCh: make(chan error),
		}

		m.qch <- task

		err := <-task.errCh
		if err != nil {
			return errorMsg{err}
		}

		return nil
	}
}

func (m queryRunnerModel) processCmd() tea.Cmd {
	return func() tea.Msg {
		select {
		case out, ok := <-m.ch:
			if ok {
				return resultMsg(out)
			}
		default:
		}

		select {
		case <-m.done:
			return doneMsg{}
		case out, ok := <-m.ch:
			if ok {
				return resultMsg(out)
			}
			return nil
		}
	}
}
