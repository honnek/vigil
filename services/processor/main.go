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
	pb "github.com/honnek/vigil/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	conn, err := grpc.NewClient(storageAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	h := consumerHandler{storage: storageClient, cb: circuitbreaker.NewCircuitBreaker(5, 10*time.Second, 5)}
	for {
		if err := cg.Consume(ctx, []string{topic}, &h); err != nil {
			log.Printf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}
