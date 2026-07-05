package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/honnek/vigil/pkg/metrics"
	pb "github.com/honnek/vigil/proto"
	"github.com/honnek/vigil/services/storage/repository"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

var (
	metricsSaved = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_storage_metrics_saved_total",
		Help: "Количество сохраненных метрик",
	})
	metricsSizeBatch = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vigil_storage_batch_size",
		Help:    "Размер батча метрик",
		Buckets: prometheus.LinearBuckets(10, 10, 10),
	})
)

func main() {
	srvMetrics := grpcprom.NewServerMetrics()
	prometheus.MustRegister(srvMetrics)
	server := grpc.NewServer(
		grpc.ChainStreamInterceptor(srvMetrics.StreamServerInterceptor()),
		grpc.ChainUnaryInterceptor(srvMetrics.UnaryServerInterceptor()),
	)
	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":2112"
	}

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://vigil:secret@localhost:5432/vigil?sslmode=disable"
		log.Println("not found POSTGRES_DSN")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := RunMigrations(ctx, dsn); err != nil {
		log.Fatal(err)
	}

	pool, err := repository.NewPool(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	repo := repository.NewPgMetricRepository(pool)
	if err := repo.EnsurePartitions(ctx, 2); err != nil {
		log.Fatal(err)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}

	cachingRepo := repository.NewCachingRepository(repo, redisClient, 30*time.Second)

	pb.RegisterStorageServiceServer(server, NewStorageService(cachingRepo))
	l, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := server.Serve(l); err != nil {
			log.Fatal(err)
		}
	}()

	metrics.Serve(metricsAddr)

	log.Printf("serving on port 9091")

	<-ctx.Done()
	server.GracefulStop()
}
