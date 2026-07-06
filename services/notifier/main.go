package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/honnek/vigil/pkg/kafka"
	"github.com/honnek/vigil/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	consumedMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_notifier_messages_consumed_total",
		Help: "Число обработанных сообщений",
	})
	errorsMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_notifier_errors_total",
		Help: "Число ошибок",
	}, []string{"stage"})
	deliveryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "vigil_notifier_delivery_duration_seconds",
		Help: "Время доставки алерта",
	})
)

const groupId = "vigil-notifier"
const consumeTopic = "alerts"

func main() {

	kafkaAddr := os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}
	token := os.Getenv("TELEGRAM_TOKEN")
	if token == "" {
		log.Fatal("token required")
	}
	chatID := os.Getenv("TELEGRAM_CHAT_ID")
	if chatID == "" {
		log.Fatal("chat id required")
	}
	prometheusMetricsAddr := os.Getenv("METRICS_ADDR")
	if prometheusMetricsAddr == "" {
		prometheusMetricsAddr = ":2112"
	}

	notifier := NewTelegramNotifier(token, chatID)

	cg, err := kafka.NewConsumerGroup(kafkaAddr, groupId)
	if err != nil {
		log.Fatal(err)
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
				log.Println("consumer error:", err)
			}
		}
	}()

	metrics.Serve(prometheusMetricsAddr)

	h := NotifierHandler{notifier: notifier}

	for {
		err := cg.Consume(ctx, []string{consumeTopic}, &h)
		if err != nil {
			log.Println("consume error:", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}
