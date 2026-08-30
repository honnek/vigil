package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	pb "github.com/honnek/vigil/proto"
)

type Notifier interface {
	Send(ctx context.Context, alert *pb.Alert) error
	SendAnomaly(ctx context.Context, a *pb.Anomaly) error
}
type TelegramNotifier struct {
	token  string
	chatID string
	http   *http.Client
}

func NewTelegramNotifier(token, chatID string) *TelegramNotifier {
	return &TelegramNotifier{
		token:  token,
		chatID: chatID,
		http:   &http.Client{Timeout: 10 * time.Second},
	}
}

func (t *TelegramNotifier) Send(ctx context.Context, alert *pb.Alert) error {
	text := fmt.Sprintf("🔥 %s на %s = %.2f (порог %.2f), правило %s",
		alert.GetMetric(), alert.GetHost(), alert.GetValue(), alert.GetThreshold(), alert.GetRule())
	err := t.SendText(ctx, text)
	if err != nil {
		return err
	}

	return nil
}

func (t *TelegramNotifier) SendAnomaly(ctx context.Context, a *pb.Anomaly) error {
	conf := a.GetConfidence() * 100
	var text string

	switch a.GetPattern() {
	case "miner":
		text = fmt.Sprintf("⛏️ Майнер: CPU-плато на %s = %.1f%% (увер. %.0f%%)",
			a.GetHost(), a.GetValue(), conf)
	case "memory_leak":
		text = fmt.Sprintf("🧠 Утечка памяти на %s: RAM %.1f%% и растёт (увер. %.0f%%)",
			a.GetHost(), a.GetValue(), conf)
	case "ransomware":
		text = fmt.Sprintf("🔒 Ransomware: всплеск записи на %s = %.1f МБ/с (увер. %.0f%%)",
			a.GetHost(), a.GetValue()/1024/1024, conf)
	default: // "zscore" и всё прочее — статистическая аномалия
		text = fmt.Sprintf("⚠️ Аномалия: %s на %s = %.2f (норма %.2f±%.2f, z=%.1f, увер. %.0f%%)",
			a.GetMetricName(), a.GetHost(), a.GetValue(), a.GetMean(), a.GetStdDev(),
			a.GetZscore(), conf)
	}

	return t.SendText(ctx, text)
}

func (t *TelegramNotifier) SendText(ctx context.Context, text string) error {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.token)
	body, err := json.Marshal(map[string]any{
		"chat_id": t.chatID,
		"text":    text,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
