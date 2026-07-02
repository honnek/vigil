package main

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/honnek/vigil/pkg/retry"
	pb "github.com/honnek/vigil/proto"
	"google.golang.org/protobuf/proto"
)

type NotifierHandler struct {
	notifier Notifier
}

var _ sarama.ConsumerGroupHandler = (*NotifierHandler)(nil)

func (h *NotifierHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *NotifierHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *NotifierHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var alert pb.Alert

		if err := proto.Unmarshal(msg.Value, &alert); err != nil {
			log.Printf("Error decoding alert: %v", err)
			session.MarkMessage(msg, "")
			continue
		}

		ctx := session.Context()
		if err := retry.Do(ctx, 5, time.Second, func() error {
			return h.notifier.Send(ctx, &alert)
		}); err != nil {
			return err
		}

		session.MarkMessage(msg, "")
	}

	return nil
}
