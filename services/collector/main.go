package main

import (
	"context"
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
	"github.com/honnek/vigil/pkg/tracing"
	pb "github.com/honnek/vigil/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type MetricsServer struct {
	pb.UnimplementedMetricsServiceServer
	producer sarama.SyncProducer
}

type LogServer struct {
	pb.UnimplementedLogIngestServer
	producer sarama.SyncProducer
}

const port = ":9090"
const topic = "metrics.raw"
const logTopic = "logs.raw"

var (
	metricsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_collector_metrics_received_total",
		Help: "Количество принятых и опубликованных метрик",
	})
	metricsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_collector_metrics_rejected_total",
		Help: "Количество отклонённых метрик по причине",
	}, []string{"reason"})

	logsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_collector_logs_received_total",
		Help: "Количество принятых и опубликованных логов",
	})
	logsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_collector_logs_rejected_total",
		Help: "Количество отклонённых логов",
	}, []string{"reason"})
)

func (ls *LogServer) StreamLogs(stream pb.LogIngest_StreamLogsServer) error {
	var received, rejected int64
	for {
		logEntry, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.LogStreamSummary{
				Received: received,
				Rejected: rejected,
			})
		}
		if err != nil {
			return err
		}

		if err := ValidateLog(logEntry); err != nil {
			logsRejected.WithLabelValues("validate").Inc()
			log.Printf("Rejected on validation: %s", err.Error())
			rejected++
			continue
		}

		data, err := proto.Marshal(logEntry)
		if err != nil {
			logsRejected.WithLabelValues("marshal").Inc()
			log.Printf("Failed to marshal log entry: %s", err.Error())
			rejected++
			continue
		}

		err = kafka.PublishMetric(stream.Context(), ls.producer, logTopic, logEntry.GetHost(), data) // имя оставлю пока
		if err != nil {
			logsRejected.WithLabelValues("publish").Inc()
			log.Printf("Failed to publish log entry: %s", err.Error())
			rejected++
			continue
		}

		logsReceived.Inc()
		received++
	}
}

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
		err = kafka.PublishMetric(stream.Context(), s.producer, topic, metric.GetHost(), data)
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

func ValidateLog(logEntry *pb.LogEntry) error {
	if logEntry.GetHost() == "" {
		return errors.New("host is required")
	}
	if nil == logEntry.GetTimestamp() {
		return errors.New("timestamp is required")
	}
	if err := logEntry.GetTimestamp().CheckValid(); err != nil {
		return err
	}
	if logEntry.GetMessage() == "" {
		return errors.New("message is required")
	}

	return nil
}

func main() {
	srvMetrics := grpcprom.NewServerMetrics()
	prometheus.MustRegister(srvMetrics)
	server := grpc.NewServer(
		grpc.ChainStreamInterceptor(srvMetrics.StreamServerInterceptor()),
		grpc.ChainUnaryInterceptor(srvMetrics.UnaryServerInterceptor()),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":2112"
	}

	otelAddr := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelAddr == "" {
		otelAddr = "localhost:4317"
	}
	shutdown, err := tracing.Init(context.Background(), "vigil-collector", otelAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	kafkaAddr := os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}
	producer, err := kafka.NewProducer(kafkaAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	ls := &LogServer{producer: producer}
	pb.RegisterLogIngestServer(server, ls)

	ms := &MetricsServer{producer: producer}
	pb.RegisterMetricsServiceServer(server, ms)
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
