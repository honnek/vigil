package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/honnek/vigil/pkg/kafka"
	"github.com/redis/go-redis/v9"
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
