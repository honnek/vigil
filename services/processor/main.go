package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/honnek/vigil/pkg/circuitbreaker"
	"github.com/honnek/vigil/pkg/kafka"
	"github.com/honnek/vigil/pkg/metrics"
	"github.com/honnek/vigil/pkg/tracing"
	pb "github.com/honnek/vigil/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	consumedMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_processor_messages_consumed_total",
		Help: "Число обработанных сообщений",
	})
	errorsMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_processor_errors_total",
		Help: "Число ошибок",
	}, []string{"stage"})
	flushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "vigil_processor_flush_duration_seconds",
		Help: "Задержка flush",
	})
)

const groupId = "vigil-processor"
const topic = "metrics.raw"

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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	otelAddr := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelAddr == "" {
		otelAddr = "localhost:4317"
	}
	shutdown, err := tracing.Init(ctx, "vigil-processor", otelAddr)
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

	cg, err := kafka.NewConsumerGroup(kafkaAddr, groupId)
	if err != nil {
		log.Fatalf("Error creating consumer group client: %v", err)
	}
	defer cg.Close()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-cg.Errors():
				log.Printf("Error from consumer: %v", err)
			}
		}
	}()

	metrics.Serve(prometheusMetricsAddr)

	agg := NewPool(5, 60, time.Minute, storageClient, ctx)
	cb := circuitbreaker.NewCircuitBreaker(5, 10*time.Second, 5)
	h := consumerHandler{storage: storageClient, agg: agg, cb: cb}
	for {
		if err := cg.Consume(ctx, []string{topic}, &h); err != nil {
			log.Printf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}
