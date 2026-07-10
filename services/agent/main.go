package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/honnek/vigil/pkg/tracing"
	pb "github.com/honnek/vigil/proto"
	"github.com/honnek/vigil/services/agent/source"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	otelAddr := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelAddr == "" {
		otelAddr = "localhost:4317"
	}
	shutdown, err := tracing.Init(ctx, "vigil-agent", otelAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	conn, err := grpc.NewClient(
		collectorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	servClient := pb.NewMetricsServiceClient(conn)

	err = collectAndSend(ctx, servClient)
	if err != nil {
		log.Fatal(err)
	}
}

func collectAndSend(ctx context.Context, client pb.MetricsServiceClient) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			tickCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			stream, err := client.StreamMetrics(tickCtx)
			if err != nil {
				log.Printf("Failed to stream metrics: %v", err)
				cancel()
				continue
			}

			for _, s := range sources {
				metrics, err := s.Collect()
				if err != nil {
					log.Println(err)
					continue
				}

				for _, metric := range metrics {
					if err := stream.Send(metric); err != nil {
						log.Println(err)
					}
				}
			}
			summary, err := stream.CloseAndRecv()
			if err != nil {
				log.Println(err)
			} else {
				log.Println(summary)
			}
			cancel()
		}

	}
}
