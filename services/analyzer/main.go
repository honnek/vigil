package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/honnek/vigil/pkg/kafka"
	"github.com/honnek/vigil/pkg/metrics"
	"github.com/honnek/vigil/pkg/tracing"
	pb "github.com/honnek/vigil/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	seriesChecked = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_analyzer_series_checked_total",
		Help: "Число проанализированных серий (host+metric) за всё время"})
	anomaliesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_analyzer_anomalies_total",
		Help: "Число обнаруженных аномалий по host и metric"}, []string{"host", "metric"})
	runDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "vigil_analyzer_run_duration_seconds",
		Help: "Длительность одного прохода анализа в секундах"})
)

func main() {
	kafkaAddr := os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}
	storageAddr := os.Getenv("STORAGE_ADDR")
	if storageAddr == "" {
		storageAddr = "localhost:9091"
	}
	prometheusMetricsAddr := os.Getenv("METRICS_ADDR")
	if prometheusMetricsAddr == "" {
		prometheusMetricsAddr = ":2112"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	otelAddr := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelAddr == "" {
		otelAddr = "localhost:4317"
	}
	shutdown, err := tracing.Init(ctx, "vigil-analyzer", otelAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	conn, err := grpc.NewClient(
		storageAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	storageClient := pb.NewStorageServiceClient(conn)

	producer, err := kafka.NewProducer(kafkaAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}

	cfg := Load()
	a := Analyzer{
		storage:  storageClient,
		rdb:      redisClient,
		producer: producer,
		cfg:      cfg,
	}

	metrics.Serve(prometheusMetricsAddr)

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.Run(ctx); err != nil {
				log.Printf("error running analyzer: %v", err)
			}
		}
	}
}
