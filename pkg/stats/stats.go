package stats

import "math"

type Number interface {
	~int | ~int64 | ~float64
}

func Mean[T Number](xs []T) (float64, bool) {
	if len(xs) == 0 {
		return 0, false
	}

	sum := 0.0
	for _, x := range xs {
		sum += float64(x)
	}

	return sum / float64(len(xs)), true
}

func StdDev[T Number](xs []T) (float64, bool) {
	if len(xs) < 2 {
		return 0, false
	}

	mean, isOk := Mean(xs)
	if !isOk {
		return 0, false
	}

	sumSqr := 0.0
	for _, x := range xs {
		sumSqr += (float64(x) - mean) * (float64(x) - mean)
	}

	return math.Sqrt(sumSqr / float64(len(xs)-1)), true
}

func ZScore(x, mean, stdDev float64) (float64, bool) {
	if stdDev == 0 {
		return 0, false
	}

	return (x - mean) / stdDev, true
}
