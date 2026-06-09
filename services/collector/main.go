package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	pb "github.com/honnek/vigil/proto"
	"google.golang.org/grpc"
)

type MetricsServer struct {
	pb.UnimplementedMetricsServiceServer
}

const port = ":9090"

func (s *MetricsServer) StreamMetrics(stream pb.MetricsService_StreamMetricsServer) error {
	var received, rejected int64
	for {
		metric, err := stream.Recv()
		if err == io.EOF {
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

		fmt.Println("received request metric: ", metric)
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
	var ms MetricsServer
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
