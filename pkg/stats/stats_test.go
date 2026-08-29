package stats

import (
	"math"
	"testing"
)

const eps = 1e-9

func TestMean(t *testing.T) {
	tests := []struct {
		name   string
		xs     []float64
		want   float64
		wantOk bool
	}{
		{"empty", []float64{}, 0, false},
		{"single", []float64{5}, 5, true},
		{"multiple", []float64{2, 4, 6}, 4, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := Mean(tt.xs)
			if gotOk != tt.wantOk {
				t.Fatalf("Mean() gotOk = %v, wantOk %v", gotOk, tt.wantOk)
			}
			if gotOk && got != tt.want {
				t.Errorf("Mean() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestStdDev(t *testing.T) {
	tests := []struct {
		name   string
		xs     []float64
		want   float64
		wantOk bool
	}{
		{"empty", []float64{}, 0, false},
		{"few", []float64{5}, 0, false},
		{"equals", []float64{5, 5, 5}, 0, true},
		{"multiple", []float64{2, 4, 6}, 2.0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := StdDev(tt.xs)

			if gotOk != tt.wantOk {
				t.Fatalf("StdDev() gotOk = %v, wantOk %v", gotOk, tt.wantOk)
			}

			if gotOk && math.Abs(got-tt.want) > eps {
				t.Errorf("StdDev() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestZScore(t *testing.T) {
	tests := []struct {
		name   string
		x      float64
		mean   float64
		stdDev float64
		want   float64
		wantOk bool
	}{
		{"zeroStdDev", 1, 1, 0, 0, false},
		{"below", 40, 50, 5, -2, true},
		{"multiple", 90, 50, 1.5, 40 / 1.5, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := ZScore(tt.x, tt.mean, tt.stdDev)

			if gotOk != tt.wantOk {
				t.Fatalf("ZScore() gotOk = %v, wantOk %v", gotOk, tt.wantOk)
			}

			if gotOk && math.Abs(got-tt.want) > eps {
				t.Errorf("ZScore() got = %v, want = %v", got, tt.want)
			}
		})
	}
}
