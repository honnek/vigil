package main

import (
	"errors"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/honnek/vigil/pkg/circuitbreaker"
	"github.com/honnek/vigil/pkg/kafka"
	pb "github.com/honnek/vigil/proto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

type consumerHandler struct {
	storage pb.StorageServiceClient
	agg     *Pool
	cb      *circuitbreaker.CircuitBreaker
}

const batchSize = 500

var _ sarama.ConsumerGroupHandler = (*consumerHandler)(nil)

func (h *consumerHandler) Setup(sess sarama.ConsumerGroupSession) error {
	return nil
}
func (h *consumerHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil
}
func (h *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	buf := make([]*pb.Metric, 0, batchSize)
	bufMsgs := make([]*sarama.ConsumerMessage, 0, batchSize)
	for msg := range claim.Messages() {
		var m pb.Metric
		consumedMessages.Inc()

		ctx := otel.GetTextMapPropagator().Extract(sess.Context(), kafka.NewConsumerCarrier(msg))
		tracer := otel.Tracer("vigil-processor")
		ctx, span := tracer.Start(ctx, "process metric", trace.WithSpanKind(trace.SpanKindConsumer))

		err := proto.Unmarshal(msg.Value, &m)
		if err != nil {
			log.Printf("Error unmarshaling message: %s\n", err)
			errorsMessages.WithLabelValues("decode").Inc()
			span.End()
			sess.MarkMessage(msg, "")
			continue
		}

		buf = append(buf, &m)
		bufMsgs = append(bufMsgs, msg)

		h.agg.submit(&m)

		if len(buf) >= batchSize {
			if err := h.flush(sess, buf, bufMsgs); err != nil {
				return err
			}

			bufMsgs = bufMsgs[:0]
			buf = buf[:0]
		}

		span.End()
	}

	if err := h.flush(sess, buf, bufMsgs); err != nil {
		return err
	}

	return nil
}

func (h *consumerHandler) flush(sess sarama.ConsumerGroupSession, buf []*pb.Metric, bufMsgs []*sarama.ConsumerMessage) error {
	if len(buf) == 0 {
		return nil
	}
	start := time.Now()

	err := h.cb.Execute(func() error {
		saveStream, err := h.storage.SaveMetrics(sess.Context())
		if err != nil {
			return err
		}
		for _, m := range buf {
			err = saveStream.Send(m)
			if err != nil {
				return err
			}
		}

		_, err = saveStream.CloseAndRecv()
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, circuitbreaker.ErrorOpen) {
			time.Sleep(5 * time.Second)
		}
		errorsMessages.WithLabelValues("execute").Inc()
		return err

	}

	flushDuration.Observe(time.Since(start).Seconds())

	for _, m := range bufMsgs {
		sess.MarkMessage(m, "")
	}
	return nil
}
