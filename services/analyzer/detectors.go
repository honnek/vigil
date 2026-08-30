package main

import (
	"time"

	"github.com/honnek/vigil/pkg/stats"
)

type Detector interface {
	Name() string
	Applies(metricName string) bool
	Detect(points []Point) (confidence float64, matched bool)
}

type MinerDetector struct {
	Threshold float64
	Duration  time.Duration
}

func (d *MinerDetector) Name() string {
	return "miner"
}
func (d *MinerDetector) Applies(metricName string) bool {
	return metricName == "cpu_usage_percent"
}

func (d *MinerDetector) Detect(points []Point) (confidence float64, matched bool) {
	if len(points) == 0 {
		return 0, false
	}
	maxTS := points[0].TS
	for _, p := range points {
		if p.TS.After(maxTS) {
			maxTS = p.TS
		}
	}

	cutoff := maxTS.Add(-d.Duration)
	latestPoints := make([]Point, 0)
	for _, p := range points {
		if !p.TS.Before(cutoff) {
			latestPoints = append(latestPoints, p)
		}
	}
	if len(latestPoints) == 0 {
		return 0, false
	}

	oldestPoint := latestPoints[0]
	for _, lp := range latestPoints {
		if oldestPoint.TS.After(lp.TS) {
			oldestPoint = lp
		}
	}
	if oldestPoint.TS.After(cutoff) {
		return 0, false
	}

	matched = true
	sum := 0.0
	for _, lp := range latestPoints {
		sum += lp.Value
		if lp.Value < d.Threshold {
			matched = false
		}
	}

	avg := sum / float64(len(latestPoints))
	confidence = (avg - d.Threshold) / (100 - d.Threshold)
	if confidence > 1 {
		confidence = 1
	}

	return confidence, matched
}

type MemoryLeakDetector struct {
	MinGrowth float64
}

func (d *MemoryLeakDetector) Name() string {
	return "memory_leak"
}

func (d *MemoryLeakDetector) Applies(metricName string) bool {
	return metricName == "used_percent"
}

func (d *MemoryLeakDetector) Detect(points []Point) (confidence float64, matched bool) {
	midIndx := len(points) / 2
	freshPoints := points[:midIndx]
	oldPoints := points[midIndx:]

	freshAvg, freshOk := stats.Mean(values(freshPoints))
	if !freshOk {
		return 0, false
	}
	oldAvg, oldOk := stats.Mean(values(oldPoints))
	if !oldOk {
		return 0, false
	}

	growth := freshAvg - oldAvg

	if growth > d.MinGrowth {
		matched = true
	}

	c := (growth - d.MinGrowth) / d.MinGrowth
	if c > 1 {
		c = 1
	}

	return c, matched
}

type RansomwareDetector struct {
	WriteThreshold float64
}

func (d *RansomwareDetector) Name() string {
	return "ransomware"
}

func (d *RansomwareDetector) Applies(metricName string) bool {
	return metricName == "disk_write_bytes_per_sec"
}

func (d *RansomwareDetector) Detect(points []Point) (confidence float64, matched bool) {
	midIndx := len(points) / 2
	freshPoints := points[:midIndx]
	freshAvg, freshOk := stats.Mean(values(freshPoints))
	if !freshOk {
		return 0, false
	}

	if freshAvg > d.WriteThreshold {
		matched = true
	}

	confidence = (freshAvg - d.WriteThreshold) / d.WriteThreshold
	if confidence > 1 {
		confidence = 1
	}

	return confidence, matched
}
