package main

import (
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
)

type alertsTab struct {
	table   table.Model
	err     error
	updated time.Time
}

func newAlertsTab() alertsTab {
	return alertsTab{
		table: newTable([]table.Column{
			{Title: "Host", Width: 24},
			{Title: "Rule", Width: 32},
		}),
	}
}

func (t alertsTab) Update(msg tea.Msg) (alertsTab, tea.Cmd) {
	switch msg := msg.(type) {
	case alertsMsg:
		t.err = nil
		t.updated = time.Now()
		rows := make([]table.Row, 0, len(msg))
		for _, a := range msg {
			rows = append(rows, table.Row{a.Host, a.Rule})
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

func (t alertsTab) View() string {
	title := titleStyle.Render(fmt.Sprintf("ACTIVE ALERTS (%d)", len(t.table.Rows())))
	return title + "  " + t.status() + "\n" + t.table.View()
}

func (t alertsTab) status() string {
	switch {
	case t.err != nil:
		return errStyle.Render("ERR: " + t.err.Error())
	case !t.updated.IsZero():
		return footerStyle.Render("updated " + t.updated.Format("15:04:05"))
	default:
		return footerStyle.Render("loading…")
	}
}

func (t *alertsTab) setSize(_, h int) {
	t.table.SetHeight(h)
}
