package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

const groupId = "vigil-processor"
const topic = "metrics.raw"

func main() {
	kafkaAddr := os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}

	conf := sarama.NewConfig()
	conf.Version = sarama.V3_6_0_0
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Consumer.Return.Errors = true

	cg, err := sarama.NewConsumerGroup([]string{kafkaAddr}, groupId, conf)
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

	h := consumerHandler{}
	for {
		if err := cg.Consume(ctx, []string{topic}, &h); err != nil {
			log.Printf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}
