package main

import (
	"fmt"
	"math"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type pane int

const (
	paneHosts pane = iota
	paneNames
)

var (
	boxStyle       = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("240")).Padding(0, 1).Width(28)
	activeBoxStyle = boxStyle.BorderForeground(lipgloss.Color("205"))
	sparkStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
)

type metricsTab struct {
	client *Client
	width  int
	height int

	hosts       []string            // уникальные хосты (левая колонка)
	namesByHost map[string][]string // host → имена метрик (правая колонка)

	focus   pane
	hostCur int
	nameCur int

	selHost string    // выбранная серия (по enter)
	selName string    //
	points  []float64 // значения последней метрики, по возрастанию времени

	err error
}

func newMetricsTab(client *Client) metricsTab {
	return metricsTab{client: client, namesByHost: map[string][]string{}}
}

// refresh — что грузить: точки выбранной метрики, иначе список серий.
func (t metricsTab) refresh() tea.Cmd {
	if t.selName != "" {
		return fetchMetrics(t.client, t.selHost, t.selName, 60)
	}
	return fetchSeries(t.client)
}

// currentNames — метрики хоста под курсором (пусто, если хостов нет).
func (t metricsTab) currentNames() []string {
	if t.hostCur < 0 || t.hostCur >= len(t.hosts) {
		return nil
	}
	return t.namesByHost[t.hosts[t.hostCur]]
}

func (t metricsTab) Update(msg tea.Msg) (metricsTab, tea.Cmd) {
	switch msg := msg.(type) {
	case seriesMsg:
		t.rebuildSeries(msg)
		t.err = nil

	case metricsMsg:
		t.points = pointsByTime(msg)
		t.err = nil

	case errMsg:
		t.err = msg.err
		return t, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "left", "h":
			t.focus = paneHosts
		case "right", "l":
			t.focus = paneNames
		case "down", "j":
			t.moveCursor(+1)
		case "up", "k":
			t.moveCursor(-1)
		case "enter":
			return t.selectMetric()
		}
	}

	return t, nil
}

// selectMetric фиксирует серию под курсором и запускает загрузку её точек.
func (t metricsTab) selectMetric() (metricsTab, tea.Cmd) {
	names := t.currentNames()
	if t.nameCur < 0 || t.nameCur >= len(names) {
		return t, nil
	}
	t.selHost = t.hosts[t.hostCur]
	t.selName = names[t.nameCur]
	t.points = nil
	return t, fetchMetrics(t.client, t.selHost, t.selName, 60)
}

// rebuildSeries пересобирает список хостов и карту host→[]name из /series.
func (t *metricsTab) rebuildSeries(series seriesMsg) {
	hostSet := map[string]bool{}
	nameSet := map[string]map[string]bool{}
	hosts := make([]string, 0)
	names := map[string][]string{}

	for _, s := range series {
		if !hostSet[s.Host] {
			hostSet[s.Host] = true
			hosts = append(hosts, s.Host)
			nameSet[s.Host] = map[string]bool{}
		}
		if !nameSet[s.Host][s.Name] {
			nameSet[s.Host][s.Name] = true
			names[s.Host] = append(names[s.Host], s.Name)
		}
	}
	sort.Strings(hosts)
	for h := range names {
		sort.Strings(names[h])
	}

	t.hosts = hosts
	t.namesByHost = names
	t.clampCursors()
}

// moveCursor двигает курсор активной колонки на d с ограничением по длине.
func (t *metricsTab) moveCursor(d int) {
	switch t.focus {
	case paneHosts:
		prev := t.hostCur
		t.hostCur = clamp(t.hostCur+d, 0, len(t.hosts)-1)
		if t.hostCur != prev {
			t.nameCur = 0                 // новый хост — правый список с начала
			t.selName, t.points = "", nil // и сбрасываем спарклайн
		}
	case paneNames:
		t.nameCur = clamp(t.nameCur+d, 0, len(t.currentNames())-1)
	}
}

// clampCursors удерживает курсоры в пределах после обновления данных.
func (t *metricsTab) clampCursors() {
	t.hostCur = clamp(t.hostCur, 0, len(t.hosts)-1)
	t.nameCur = clamp(t.nameCur, 0, len(t.currentNames())-1)
}

func clamp(v, lo, hi int) int {
	if hi < lo {
		return lo // пустой список
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// pointsByTime сортирует метрики по возрастанию времени и возвращает значения.
func pointsByTime(ms metricsMsg) []float64 {
	sorted := make([]Metric, len(ms))
	copy(sorted, ms)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].TS.Before(sorted[j].TS) })

	out := make([]float64, 0, len(sorted))
	for _, m := range sorted {
		out = append(out, m.Value)
	}
	return out
}

// sparkline рисует значения блоками ▁▂▃▄▅▆▇█.
func sparkline(vals []float64) string {
	if len(vals) == 0 {
		return ""
	}
	blocks := []rune("▁▂▃▄▅▆▇█")
	min, max := vals[0], vals[0]
	for _, v := range vals {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	var b strings.Builder
	for _, v := range vals {
		idx := len(blocks) / 2 // плоская линия, если max==min
		if max > min {
			idx = int(math.Round((v - min) / (max - min) * float64(len(blocks)-1)))
		}
		b.WriteRune(blocks[idx])
	}
	return b.String()
}

func (t metricsTab) View() string {
	title := titleStyle.Render("METRICS")

	if t.err != nil {
		return title + "\n\n" + errStyle.Render("ERR: "+t.err.Error())
	}
	if len(t.hosts) == 0 {
		return title + "\n\n" + footerStyle.Render("no series")
	}

	hostsBox := renderList("Hosts", t.hosts, t.hostCur, t.focus == paneHosts)
	namesBox := renderList(t.hosts[t.hostCur]+" · metrics", t.currentNames(), t.nameCur, t.focus == paneNames)
	cols := lipgloss.JoinHorizontal(lipgloss.Top, hostsBox, namesBox)

	help := footerStyle.Render("←/→ column · ↑/↓ move · enter: select")
	return title + "\n\n" + cols + "\n" + t.sparkView() + "\n" + help
}

// sparkView — блок со спарклайном выбранной метрики (пусто, если не выбрана).
func (t metricsTab) sparkView() string {
	if t.selName == "" {
		return ""
	}
	head := titleStyle.Render(t.selHost + " · " + t.selName + " (last 15m)")
	if len(t.points) == 0 {
		return "\n" + head + "\n" + footerStyle.Render("loading…")
	}
	last := fmt.Sprintf("%.2f", t.points[len(t.points)-1])
	return "\n" + head + "\n" + sparkStyle.Render(sparkline(t.points)) + "  " + last
}

// renderList рисует одну колонку-список в рамке; active подсвечивает рамку.
func renderList(header string, items []string, cursor int, active bool) string {
	var b strings.Builder
	b.WriteString(titleStyle.Render(header) + "\n\n")
	if len(items) == 0 {
		b.WriteString(footerStyle.Render("—"))
	}
	for i, it := range items {
		if i == cursor {
			b.WriteString(titleStyle.Render("▸ " + it))
		} else {
			b.WriteString("  " + it)
		}
		if i < len(items)-1 {
			b.WriteString("\n")
		}
	}

	style := boxStyle
	if active {
		style = activeBoxStyle
	}
	return style.Render(b.String())
}

func (t *metricsTab) setSize(w, h int) {
	t.width, t.height = w, h
}
