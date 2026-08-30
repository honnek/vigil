package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/honnek/vigil/pkg/kafka"
	"github.com/honnek/vigil/pkg/stats"
	pb "github.com/honnek/vigil/proto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Analyzer struct {
	storage   pb.StorageServiceClient
	rdb       *redis.Client
	producer  sarama.SyncProducer
	Detectors []Detector
	cfg       Config
}

type Point struct {
	Value float64
	TS    time.Time
}

const anomalyTopic = "anomalies"

func (a *Analyzer) Run(ctx context.Context) error {
	start := time.Now()
	defer func() { runDuration.Observe(time.Since(start).Seconds()) }()

	now := time.Now()
	resp, err := a.storage.ListSeries(
		ctx,
		&pb.ListSeriesRequest{Since: timestamppb.New(now.Add(-a.cfg.Window))},
	)
	if err != nil {
		return err
	}

	for _, s := range resp.GetSeries() {
		if skip(s.GetName()) {
			continue
		}
		seriesChecked.Inc()
		all, err := a.fetchWindow(ctx, s)
		if err != nil {
			log.Printf("fetchWindow %s/%s: %v", s.GetHost(), s.GetName(), err)
			continue
		}
		if len(all) < a.cfg.MinPoints {
			continue
		}

		latest := all[0].Value // новейшая точка — проверяемая
		history := all[1:]     // baseline без новейшей

		// --- Z-score baseline ---
		vals := values(history)
		mean, ok1 := stats.Mean(vals)
		sd, ok2 := stats.StdDev(vals)
		z, ok3 := stats.ZScore(latest, mean, sd)
		if ok1 && ok2 && ok3 && math.Abs(z) > a.cfg.ZThreshold {
			if a.allowPublish(ctx, s, "zscore", z) {
				a.publish(ctx, s, latest, mean, sd, z, confidence(z, a.cfg.ZThreshold), "zscore")
			}
		}

		// --- Детекторы паттернов (майнер и т.д.) ---
		for _, d := range a.Detectors {
			if !d.Applies(s.GetName()) {
				continue
			}
			conf, matched := d.Detect(all) // полное окно
			if !matched {
				continue
			}
			if a.allowPublish(ctx, s, d.Name(), conf) {
				a.publish(ctx, s, latest, 0, 0, 0, conf, d.Name())
			}
		}
	}

	return nil
}

// allowPublish — дедуп по ключу host:metric:pattern. Redis недоступен → шлём (без дедупа).
func (a *Analyzer) allowPublish(ctx context.Context, s *pb.Series, pattern string, val float64) bool {
	key := fmt.Sprintf("anomaly:%s:%s:%s", s.GetHost(), s.GetName(), pattern)
	isNew, err := a.rdb.SetNX(ctx, key, val, a.cfg.DedupTTL).Result()
	if err != nil {
		log.Printf("SetNX %s: %v", key, err)
		return true
	}
	return isNew
}

func (a *Analyzer) publish(ctx context.Context, s *pb.Series, val, mean, sd, z, conf float64, pattern string) {
	anomaly := pb.Anomaly{
		Host:       s.Host,
		MetricName: s.Name,
		Value:      val,
		Mean:       mean,
		StdDev:     sd,
		Zscore:     z,
		Confidence: conf,
		Pattern:    pattern,
		Timestamp:  timestamppb.New(time.Now()),
	}
	pubMsg, _ := proto.Marshal(&anomaly)
	anomaliesTotal.WithLabelValues(s.Host, s.Name).Inc()
	err := kafka.PublishMetric(ctx, a.producer, anomalyTopic, s.Host, pubMsg)
	if err != nil {
		fmt.Printf("Failed to publish anomaly to Kafka: %v\n", err)
	}
}

// fetchWindow возвращает полное окно точек серии (DESC: [0] — новейшая).
func (a *Analyzer) fetchWindow(ctx context.Context, s *pb.Series) ([]Point, error) {
	stream, err := a.storage.ListMetrics(ctx, &pb.ListMetricsRequest{
		Host: s.Host,
		Name: s.Name,
		From: timestamppb.New(time.Now().Add(-a.cfg.Window)),
		To:   timestamppb.New(time.Now()),
	})
	if err != nil {
		return nil, err
	}
	all := make([]Point, 0)

	for {
		metric, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			return nil, recvErr
		}

		all = append(all, Point{Value: metric.GetValue(), TS: metric.GetTimestamp().AsTime()})
	}

	return all, nil
}

func skip(name string) bool {
	return strings.HasSuffix(name, ":avg")
}
func confidence(z, threshold float64) float64 {
	az := math.Abs(z)
	if az <= threshold {
		return 0
	}
	c := (az - threshold) / threshold
	if c > 1 {
		c = 1
	}
	return c
}

func values(points []Point) []float64 {
	vs := make([]float64, len(points))
	for i, point := range points {
		vs[i] = point.Value
	}
	return vs
}
