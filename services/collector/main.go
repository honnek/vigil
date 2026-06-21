package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/IBM/sarama"
	pb "github.com/honnek/vigil/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type MetricsServer struct {
	pb.UnimplementedMetricsServiceServer
	producer sarama.SyncProducer
}

const port = ":9090"
const topic = "metrics.raw"

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

		data, err := proto.Marshal(metric)
		if err != nil {
			log.Printf("Failed to marshal metric: %s", err.Error())
			continue
		}
		err = publishMetric(data, s.producer, topic, metric.GetHost())
		if err != nil {
			log.Printf("Failed to publish metric: %s", err.Error())
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
	kafkaAddr := os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}
	producer, err := newProducer(kafkaAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	ms := MetricsServer{producer: producer}
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
