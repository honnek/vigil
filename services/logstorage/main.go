package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/honnek/vigil/pkg/kafka"
	"github.com/honnek/vigil/pkg/metrics"
	"github.com/honnek/vigil/pkg/tracing"
	pb "github.com/honnek/vigil/proto"
	logrepository "github.com/honnek/vigil/services/logstorage/repository"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

var (
	consumedMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_logstorage_consumed_total",
		Help: "Число обработанных сообщений",
	})
	errorsMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_logstorage_errors_total",
		Help: "Число ошибок",
	}, []string{"stage"})
	flushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "vigil_logstorage_flush_duration_seconds",
		Help: "Задержка flush",
	})
)

const logTopic = "logs.raw"

func main() {
	kafkaAddr := os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}
	chAddr := os.Getenv("CH_ADDR")
	if chAddr == "" {
		chAddr = "localhost:9000" // native TCP ClickHouse
	}
	prometheusMetricsAddr := os.Getenv("METRICS_ADDR")
	if prometheusMetricsAddr == "" {
		prometheusMetricsAddr = ":2112"
	}
	otelAddr := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelAddr == "" {
		otelAddr = "localhost:4317"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdown, err := tracing.Init(ctx, "vigil-logstorage", otelAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	dsn := os.Getenv("CLICKHOUSE_DSN")
	if dsn == "" {
		log.Fatal("not found CLICKHOUSE_DSN")
	}
	db := os.Getenv("CLICKHOUSE_DB")
	if db == "" {
		log.Fatal("not found CLICKHOUSE_DB")
	}
	user := os.Getenv("CLICKHOUSE_USER")
	if user == "" {
		log.Fatal("not found CLICKHOUSE_USER")
	}
	pass := os.Getenv("CLICKHOUSE_PASS")
	if pass == "" {
		log.Fatal("not found CLICKHOUSE_PASS")
	}
	grpcAddr := os.Getenv("GRPC_ADDR")
	if grpcAddr == "" {
		grpcAddr = ":9095"
	}

	if err := RunMigrations(ctx, dsn); err != nil {
		log.Fatal(err)
	}

	conn, err := logrepository.NewConnection(ctx, chAddr, db, user, pass)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	repo := logrepository.NewCHLogRepository(conn)
	h := NewConsumerLogHandler(repo)

	cg, err := kafka.NewConsumerGroup(kafkaAddr, "vigil-logstorage")
	if err != nil {
		log.Fatalf("Error creating consumer group client: %v", err)
	}
	defer cg.Close()

	srv := grpcprom.NewServerMetrics()
	prometheus.MustRegister(srv)
	grpcServer := grpc.NewServer(
		grpc.ChainStreamInterceptor(srv.StreamServerInterceptor()),
		grpc.ChainUnaryInterceptor(srv.UnaryServerInterceptor()),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	logsQueryServer := NewLogsQueryService(repo)
	pb.RegisterLogsQueryServer(grpcServer, logsQueryServer)

	listen, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := grpcServer.Serve(listen); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	metrics.Serve(prometheusMetricsAddr)

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

	for {
		if err := cg.Consume(ctx, []string{logTopic}, h); err != nil {
			log.Printf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			break
		}
	}
	grpcServer.GracefulStop()
}
