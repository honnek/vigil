package main

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Interval   time.Duration
	Window     time.Duration
	ZThreshold float64
	MinPoints  int
	DedupTTL   time.Duration
}

func Load() Config {
	return Config{
		Interval:   envDuration("ANALYZE_INTERVAL", 30*time.Second),
		Window:     envDuration("WINDOW", 30*time.Minute),
		ZThreshold: envFloat("ZSCORE_THRESHOLD", 3.0),
		MinPoints:  envInt("MIN_POINTS", 20),
		DedupTTL:   envDuration("ANOMALY_DEDUP_TTL", 10*time.Minute),
	}
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
