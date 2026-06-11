package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	pb "github.com/honnek/vigil/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MetricsServer struct {
	pb.UnimplementedMetricsServiceServer
	storage pb.StorageServiceClient
}

const port = ":9090"

func (s *MetricsServer) StreamMetrics(stream pb.MetricsService_StreamMetricsServer) error {
	var received, rejected int64
	saveStream, err := s.storage.SaveMetrics(stream.Context())
	if err != nil {
		return err
	}

	for {
		metric, err := stream.Recv()
		if err == io.EOF {
			_, err = saveStream.CloseAndRecv()
			if err != nil {
				return err
			}

			return stream.SendAndClose(&pb.StreamSummary{
				Received: received,
				Rejected: rejected,
			})
		}
		if err != nil {
			return err
		}

		if err := Validate(metric); err != nil {
			log.Printf("Rejected on validation: %s", err.Error())
			rejected++
			continue
		}

		if err := saveStream.Send(metric); err != nil {
			return err
		}

		received++
	}
}

func Validate(metric *pb.Metric) error {
	if metric.GetHost() == "" {
		return errors.New("host is required")
	}
	if nil == metric.GetTimestamp() {
		return errors.New("timestamp is required")
	}
	if err := metric.GetTimestamp().CheckValid(); err != nil {
		return err
	}
	if metric.GetType() == pb.MetricType_METRIC_TYPE_UNSPECIFIED {
		return errors.New("metric type is unspecified")
	}
	if metric.GetType() == pb.MetricType_METRIC_TYPE_CPU && metric.GetValue() < 0 {
		return errors.New("cpu metric value is negative")
	}

	return nil
}

func main() {
	server := grpc.NewServer()
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

	ms := MetricsServer{storage: storageClient}
	pb.RegisterMetricsServiceServer(server, &ms)
	listen, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Serving requests...")
	err = server.Serve(listen)
	if err != nil {
		log.Fatal(err)
		return
	}
}
