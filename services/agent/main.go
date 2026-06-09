package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/honnek/vigil/proto"
	"github.com/honnek/vigil/services/agent/source"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var sources = []source.Source{
	&source.CPUSource{},
	&source.RAMSource{},
	&source.DiskSource{},
}

func main() {
	collectorAddr := os.Getenv("COLLECTOR_ADDR")
	if collectorAddr == "" {
		collectorAddr = "localhost:9090"
	}
	conn, err := grpc.NewClient(collectorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	servClient := pb.NewMetricsServiceClient(conn)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	stream, err := servClient.StreamMetrics(streamCtx)
	if err != nil {
		log.Fatal(err)
	}

	err = collectAndSend(ctx, stream)
	if err != nil {
		log.Fatal(err)
	}
}

func collectAndSend(ctx context.Context, stream pb.MetricsService_StreamMetricsClient) error {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			summary, err := stream.CloseAndRecv()
			log.Printf("Shutting down, summary: %v err - %v", summary, err)
			return nil
		case <-ticker.C:
			for _, s := range sources {
				metrics, err := s.Collect()
				if err != nil {
					log.Println(err)
					continue
				}

				for _, metric := range metrics {
					if err := stream.Send(metric); err != nil {
						log.Println(err)
						_, recvErr := stream.CloseAndRecv()
						return recvErr
					}
				}
			}
		}
	}
}
