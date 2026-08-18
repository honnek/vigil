package main

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	pb "github.com/honnek/vigil/proto"
	logrepository "github.com/honnek/vigil/services/logstorage/repository"
	"google.golang.org/protobuf/proto"
)

type ConsumerLogHandler struct {
	repo *logrepository.CHLogRepository
}

const batchSize = 500

var _ sarama.ConsumerGroupHandler = (*ConsumerLogHandler)(nil)

func NewConsumerLogHandler(repo *logrepository.CHLogRepository) *ConsumerLogHandler {
	return &ConsumerLogHandler{
		repo: repo,
	}
}

func (h *ConsumerLogHandler) Setup(sess sarama.ConsumerGroupSession) error {
	return nil
}
func (h *ConsumerLogHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerLogHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	buf := make([]*pb.LogEntry, 0, batchSize)
	bufMsgs := make([]*sarama.ConsumerMessage, 0, batchSize)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				// канал закрыт (ребаланс/выключение) — флашим остаток и выходим
				return h.flush(sess, buf, bufMsgs)
			}

			var le pb.LogEntry
			consumedMessages.Inc()

			if err := proto.Unmarshal(msg.Value, &le); err != nil {
				log.Printf("Error unmarshaling message: %s\n", err)
				errorsMessages.WithLabelValues("decode").Inc()
				sess.MarkMessage(msg, "")
				continue
			}

			buf = append(buf, &le)
			bufMsgs = append(bufMsgs, msg)

			if len(buf) >= batchSize {
				if err := h.flush(sess, buf, bufMsgs); err != nil {
					return err
				}
				bufMsgs = bufMsgs[:0]
				buf = buf[:0]
			}

		case <-ticker.C:
			// периодический флаш: не держим логи в буфере при низком трафике
			if err := h.flush(sess, buf, bufMsgs); err != nil {
				return err
			}
			bufMsgs = bufMsgs[:0]
			buf = buf[:0]

		case <-sess.Context().Done():
			return h.flush(sess, buf, bufMsgs)
		}
	}
}

func (h *ConsumerLogHandler) flush(sess sarama.ConsumerGroupSession, buf []*pb.LogEntry, bufMsgs []*sarama.ConsumerMessage) error {
	if len(buf) == 0 {
		return nil
	}
	start := time.Now()

	err := h.repo.SaveBatch(sess.Context(), buf)
	if err != nil {
		errorsMessages.WithLabelValues("save").Inc()
		return err
	}

	flushDuration.Observe(time.Since(start).Seconds())
	for _, msg := range bufMsgs {
		sess.MarkMessage(msg, "")
	}

	return nil
}
