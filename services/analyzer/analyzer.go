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
	storage  pb.StorageServiceClient
	rdb      *redis.Client
	producer sarama.SyncProducer
	cfg      Config
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
		vals, last, err := a.fetchWindow(ctx, s)
		if err != nil {
			log.Printf("fetchWindow %s/%s: %v", s.GetHost(), s.GetName(), err)
			continue
		}
		if len(vals) < a.cfg.MinPoints {
			continue
		}

		mean, ok1 := stats.Mean(vals)
		sd, ok2 := stats.StdDev(vals)
		if !ok1 || !ok2 {
			continue
		}
		z, ok3 := stats.ZScore(last, mean, sd)
		if !ok3 {
			continue
		}

		if math.Abs(z) > a.cfg.ZThreshold {
			key := fmt.Sprintf("anomaly:%s:%s", s.GetHost(), s.GetName())
			isNew, err := a.rdb.SetNX(ctx, key, z, a.cfg.DedupTTL).Result()
			if err != nil {
				// Redis недоступен → шлём без дедупа (лучше спам, чем пропуск)
				log.Printf("SetNX %s: %v", key, err)
			} else if !isNew {
				continue // дубль в пределах TTL — молчим
			}

			a.publish(ctx, s, last, mean, sd, z)
		}
	}

	return nil
}

func (a *Analyzer) publish(ctx context.Context, s *pb.Series, val, mean, sd, z float64) {
	confidence := confidence(z, a.cfg.ZThreshold)
	anomaly := pb.Anomaly{
		Host:       s.Host,
		MetricName: s.Name,
		Value:      val,
		Mean:       mean,
		StdDev:     sd,
		Zscore:     z,
		Confidence: confidence,
		Timestamp:  timestamppb.New(time.Now()),
	}
	pubMsg, _ := proto.Marshal(&anomaly)
	anomaliesTotal.WithLabelValues(s.Host, s.Name).Inc()
	err := kafka.PublishMetric(ctx, a.producer, anomalyTopic, s.Host, pubMsg)
	if err != nil {
		fmt.Printf("Failed to publish anomaly to Kafka: %v\n", err)
	}
}

func (a *Analyzer) fetchWindow(ctx context.Context, s *pb.Series) (history []float64, latest float64, err error) {
	stream, err := a.storage.ListMetrics(ctx, &pb.ListMetricsRequest{
		Host: s.Host,
		Name: s.Name,
		From: timestamppb.New(time.Now().Add(-a.cfg.Window)),
		To:   timestamppb.New(time.Now()),
	})
	if err != nil {
		return nil, 0, err
	}
	all := make([]float64, 0)

	for {
		metric, recvErr := stream.Recv()
		if recvErr == io.EOF {
			err = nil
			break
		}
		if recvErr != nil {
			return nil, 0, recvErr
		}
		all = append(all, metric.Value)
	}

	if len(all) < 2 {
		return nil, 0, nil
	}
	latest = all[0]
	history = all[1:]

	return
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
