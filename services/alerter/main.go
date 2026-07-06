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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
)

var (
	consumedMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_alerter_messages_consumed_total",
		Help: "Число обработанных сообщений",
	})
	errorsMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_alerter_errors_total",
		Help: "Число ошибок",
	}, []string{"stage"})
	flushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "vigil_alerter_flush_duration_seconds",
		Help: "Задержка flush",
	})
)

const consumeTopic = "metrics.raw"
const groupId = "vigil-alerter"

func main() {
	kafkaAddr := os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	prometheusMetricsAddr := os.Getenv("METRICS_ADDR")
	if prometheusMetricsAddr == "" {
		prometheusMetricsAddr = ":2112"
	}

	cg, err := kafka.NewConsumerGroup(kafkaAddr, groupId)
	if err != nil {
		log.Fatalf("Error creating consumer group client: %v", err)
	}
	defer cg.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}

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

	producer, err := kafka.NewProducer(kafkaAddr)
	if err != nil {
		log.Fatalf("Error creating producer: %v", err)
	}
	defer producer.Close()

	h := alerterHandler{rdb: redisClient, producer: producer, dedupTTL: 90 * time.Second, renotifyTTL: time.Minute}
	for {
		if err := cg.Consume(ctx, []string{consumeTopic}, &h); err != nil {
			log.Printf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}
