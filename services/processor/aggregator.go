package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"strings"
	"time"

	pb "github.com/honnek/vigil/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type window struct {
	buf  []float64
	pos  int
	full bool
}

type series struct {
	win    *window
	sample *pb.Metric
}

type worker struct {
	metrics    chan *pb.Metric
	series     map[string]*series
	windowSize int
	storage    pb.StorageServiceClient
	ticker     *time.Ticker
}

type Pool struct {
	workers []*worker
	n       int
}

func newWindow(size int) *window {
	return &window{buf: make([]float64, size), pos: 0, full: false}
}

func NewPool(n, windowSize int, flushInterval time.Duration, storage pb.StorageServiceClient, ctx context.Context) *Pool {
	workers := make([]*worker, n)

	for i := 0; i < n; i++ {
		w := &worker{
			metrics:    make(chan *pb.Metric, 100),
			series:     make(map[string]*series),
			windowSize: windowSize,
			storage:    storage,
			ticker:     time.NewTicker(flushInterval),
		}

		w.start(ctx)
		workers[i] = w
	}

	return &Pool{workers: workers, n: n}
}

func (p *Pool) submit(m *pb.Metric) {
	idx := hashHost(m.GetHost()) % p.n

	select {
	case p.workers[idx].metrics <- m:
	default:
	}
}

func hashHost(host string) int {
	h := fnv.New32a()
	h.Write([]byte(host))

	return int(h.Sum32())
}

func (w *window) add(v float64) {
	w.buf[w.pos] = v
	w.pos++
	if w.pos >= len(w.buf) {
		w.full = true
		w.pos = 0
	}
}

func (w *window) average() float64 {
	count := len(w.buf)
	if !w.full {
		count = w.pos
	}

	if count == 0 {
		return 0
	}

	var sum float64

	for _, value := range w.buf {
		sum += value
	}

	return sum / float64(count)
}

func (w *worker) start(ctx context.Context) {
	go func() {
		for {
			select {
			case m := <-w.metrics:
				key := SeriesKey(m)
				if _, ok := w.series[key]; !ok {
					w.series[key] = &series{win: newWindow(w.windowSize), sample: m}
				}
				w.series[key].win.add(m.GetValue())
			case <-w.ticker.C:
				saveStream, err := w.storage.SaveMetrics(ctx)
				if err != nil {
					log.Printf("failed to save metrics: %v", err)
					continue
				}

				for _, ser := range w.series {
					sample := ser.sample
					m := &pb.Metric{
						Host:      sample.GetHost(),
						Value:     ser.win.average(),
						Name:      sample.GetName() + ":avg",
						Labels:    sample.GetLabels(),
						Type:      sample.GetType(),
						Timestamp: timestamppb.Now(),
					}
					err = saveStream.Send(m)
					if err != nil {
						log.Printf("failed to send metrics: %v", err)
					}
				}

				if _, err := saveStream.CloseAndRecv(); err != nil {
					log.Printf("failed to close metrics: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func SeriesKey(m *pb.Metric) string {
	labels := make([]string, 0, len(m.GetLabels()))
	for k, v := range m.GetLabels() {
		labels = append(labels, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(labels)

	return fmt.Sprintf("%s:%s:%s", m.GetHost(), m.GetName(), strings.Join(labels, ","))
}
