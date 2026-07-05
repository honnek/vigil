package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/IBM/sarama"
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/honnek/vigil/pkg/kafka"
	"github.com/honnek/vigil/pkg/metrics"
	pb "github.com/honnek/vigil/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type MetricsServer struct {
	pb.UnimplementedMetricsServiceServer
	producer sarama.SyncProducer
}

const port = ":9090"
const topic = "metrics.raw"

var (
	metricsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_collector_metrics_received_total",
		Help: "Количество принятых и опубликованных метрик",
	})
	metricsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_collector_metrics_rejected_total",
		Help: "Количество отклонённых метрик по причине",
	}, []string{"reason"})
)

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
			metricsRejected.WithLabelValues("validate").Inc()
			log.Printf("Rejected on validation: %s", err.Error())
			rejected++
			continue
		}

		data, err := proto.Marshal(metric)
		if err != nil {
			metricsRejected.WithLabelValues("marshal").Inc()
			log.Printf("Failed to marshal metric: %s", err.Error())
			rejected++
			continue
		}
		err = kafka.PublishMetric(s.producer, topic, metric.GetHost(), data)
		if err != nil {
			metricsRejected.WithLabelValues("publish").Inc()
			log.Printf("Failed to publish metric: %s", err.Error())
			rejected++
			continue
		}

		metricsReceived.Inc()
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

	kafkaAddr := os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}
	producer, err := kafka.NewProducer(kafkaAddr)
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
	metrics.Serve(metricsAddr)
	err = server.Serve(listen)
	if err != nil {
		log.Fatal(err)
		return
	}
}
