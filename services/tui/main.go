package main

import (
	"log"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	apiAddr := os.Getenv("API_ADDR")
	if apiAddr == "" {
		apiAddr = "http://localhost:8080"
	}
	user := os.Getenv("VIGIL_USER")
	pass := os.Getenv("VIGIL_PASSWORD")
	if user == "" || pass == "" {
		log.Fatal("VIGIL_USER and VIGIL_PASSWORD environment variables not set")
	}
	refresh := os.Getenv("REFRESH_INTERVAL")
	if refresh == "" {
		refresh = "3s"
	}
	refreshInterval, err := time.ParseDuration(refresh)
	if err != nil {
		log.Printf("Failed to parse REFRESH_INTERVAL: %v (SET DEFAULT)", err)
		refreshInterval = 3 * time.Second
	}

	client, err := Login(apiAddr, user, pass)
	if err != nil {
		log.Fatal(err)
	}

	f, _ := tea.LogToFile("debug.log", "tui")
	defer f.Close()

	p := tea.NewProgram(initialModel(client, refreshInterval), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
