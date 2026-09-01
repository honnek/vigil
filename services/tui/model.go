package main

import (
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ---- сообщения и команды (общие для вкладок) ----

type alertsMsg []Alert
type logsMsg []Log
type errMsg struct{ err error }
type tickMsg time.Time

func fetchAlerts(c *Client) tea.Cmd {
	return func() tea.Msg {
		items, err := c.Alerts()
		if err != nil {
			return errMsg{err}
		}
		return alertsMsg(items)
	}
}

func fetchLogs(c *Client) tea.Cmd {
	return func() tea.Msg {
		items, err := c.Logs(100)
		if err != nil {
			return errMsg{err}
		}
		return logsMsg(items)
	}
}

func fetchSeries(c *Client) tea.Cmd {
	return func() tea.Msg {
		items, err := c.Series()
		if err != nil {
			return errMsg{err}
		}
		return seriesMsg(items)
	}
}

func fetchMetrics(c *Client, host, name string, limit int64) tea.Cmd {
	return func() tea.Msg {
		to := time.Now()
		from := to.Add(-15 * time.Minute)
		items, err := c.Metrics(host, name, from, to, limit)
		if err != nil {
			return errMsg{err}
		}

		return metricsMsg(items)
	}
}

func tick(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg { return tickMsg(t) })
}

// ---- стили ----

var (
	activeTabStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205")).Padding(0, 2)
	inactiveTabStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Padding(0, 2)
	titleStyle       = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	footerStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	errStyle         = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("196"))
	docStyle         = lipgloss.NewStyle().Padding(1, 2)
)

// newTable — таблица с общими стилями (шапка + подсветка строки).
func newTable(cols []table.Column) table.Model {
	t := table.New(table.WithColumns(cols), table.WithFocused(true))
	s := table.DefaultStyles()
	s.Header = s.Header.
		Bold(true).
		Foreground(lipgloss.Color("205")).
		BorderStyle(lipgloss.NormalBorder()).
		BorderBottom(true)
	s.Selected = s.Selected.
		Bold(true).
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57"))
	t.SetStyles(s)
	return t
}

// ---- metrics ----

type Series struct {
	Host string `json:"host"`
	Name string `json:"name"`
}

type seriesMsg []Series

type Metric struct {
	Host   string            `json:"host"`
	Name   string            `json:"name"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels"`
	TS     time.Time         `json:"ts"`
}

type metricsMsg []Metric

// ---- корневая модель: роутинг вкладок ----

type tab int

const (
	tabAlerts tab = iota
	tabLogs
	tabMetrics
	tabCount // счётчик вкладок, для переключения по кругу
)

type Model struct {
	client   *Client
	interval time.Duration
	active   tab
	width    int
	height   int

	alerts  alertsTab
	logs    logsTab
	metrics metricsTab
}

func initialModel(client *Client, interval time.Duration) tea.Model {
	return &Model{
		client:   client,
		interval: interval,
		active:   tabAlerts,
		alerts:   newAlertsTab(),
		logs:     newLogsTab(),
		metrics:  newMetricsTab(client),
	}
}

func (m *Model) Init() tea.Cmd {
	return tea.Batch(m.refreshActive(), tick(m.interval))
}

// refreshActive — команда загрузки данных активной вкладки.
func (m *Model) refreshActive() tea.Cmd {
	switch m.active {
	case tabAlerts:
		return fetchAlerts(m.client)
	case tabLogs:
		return fetchLogs(m.client)
	case tabMetrics:
		return m.metrics.refresh() // серии, а при выбранной метрике — её точки
	default:
		return nil
	}
}

func (m *Model) switchTo(t tab) tea.Cmd {
	if t == m.active {
		return nil
	}
	m.active = t
	return m.refreshActive()
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "r":
			return m, m.refreshActive()
		case "1":
			return m, m.switchTo(tabAlerts)
		case "2":
			return m, m.switchTo(tabLogs)
		case "3":
			return m, m.switchTo(tabMetrics)
		case "tab":
			return m, m.switchTo((m.active + 1) % tabCount)
		}

	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.propagateSize()
		return m, nil

	case tickMsg:
		return m, tea.Batch(m.refreshActive(), tick(m.interval))
	}

	// всё остальное (данные, навигация ↑/↓) — активной вкладке
	return m, m.updateActive(msg)
}

// updateActive делегирует msg активной вкладке и сохраняет её новое состояние.
func (m *Model) updateActive(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	switch m.active {
	case tabAlerts:
		m.alerts, cmd = m.alerts.Update(msg)
	case tabLogs:
		m.logs, cmd = m.logs.Update(msg)
	case tabMetrics:
		m.metrics, cmd = m.metrics.Update(msg)
	}
	return cmd
}

// propagateSize раздаёт вкладкам высоту под таблицу (минус табы, футер, паддинг).
func (m *Model) propagateSize() {
	h := m.height - 8
	if h < 3 {
		h = 3
	}
	m.alerts.setSize(m.width, h)
	m.logs.setSize(m.width, h)
	m.metrics.setSize(m.width, h)
}

func (m *Model) View() string {
	var body string
	switch m.active {
	case tabAlerts:
		body = m.alerts.View()
	case tabLogs:
		body = m.logs.View()
	case tabMetrics:
		body = m.metrics.View()
	}

	help := footerStyle.Render("↑/↓ nav · 1/2/3 or tab: switch · r: refresh · q: quit")
	return docStyle.Render(m.renderTabs() + "\n\n" + body + "\n\n" + help)
}

func (m *Model) renderTabs() string {
	names := []string{"1 Alerts", "2 Logs", "3 Metrics"}
	parts := make([]string, 0, len(names))
	for i, n := range names {
		if tab(i) == m.active {
			parts = append(parts, activeTabStyle.Render(n))
		} else {
			parts = append(parts, inactiveTabStyle.Render(n))
		}
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, parts...)
}
