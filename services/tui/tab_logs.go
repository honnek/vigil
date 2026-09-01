package main

import (
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
)

type logsTab struct {
	table   table.Model
	err     error
	updated time.Time
}

func newLogsTab() logsTab {
	return logsTab{
		table: newTable([]table.Column{
			{Title: "Time", Width: 8},
			{Title: "Level", Width: 6},
			{Title: "Service", Width: 14},
			{Title: "Host", Width: 14},
			{Title: "Message", Width: 48},
		}),
	}
}

func (t logsTab) Update(msg tea.Msg) (logsTab, tea.Cmd) {
	switch msg := msg.(type) {
	case logsMsg:
		t.err = nil
		t.updated = time.Now()
		rows := make([]table.Row, 0, len(msg))
		for _, l := range msg {
			rows = append(rows, table.Row{
				l.TS.Format("15:04:05"),
				l.Level,
				l.Service,
				l.Host,
				l.Message,
			})
		}
		t.table.SetRows(rows)
		return t, nil

	case errMsg:
		t.err = msg.err
		return t, nil
	}

	var cmd tea.Cmd
	t.table, cmd = t.table.Update(msg)
	return t, cmd
}

func (t logsTab) View() string {
	title := titleStyle.Render(fmt.Sprintf("LOGS (%d)", len(t.table.Rows())))
	return title + "  " + t.status() + "\n" + t.table.View()
}

func (t logsTab) status() string {
	switch {
	case t.err != nil:
		return errStyle.Render("ERR: " + t.err.Error())
	case !t.updated.IsZero():
		return footerStyle.Render("updated " + t.updated.Format("15:04:05"))
	default:
		return footerStyle.Render("loading…")
	}
}

func (t *logsTab) setSize(_, h int) {
	t.table.SetHeight(h)
}
