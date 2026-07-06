package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	pb "github.com/honnek/vigil/proto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type alerterHandler struct {
	rdb         *redis.Client
	producer    sarama.SyncProducer
	dedupTTL    time.Duration
	renotifyTTL time.Duration
}

const pubTopic = "alerts"

var _ sarama.ConsumerGroupHandler = (*alerterHandler)(nil)

func (h *alerterHandler) Setup(sess sarama.ConsumerGroupSession) error {
	return nil
}
func (h *alerterHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil
}
func (h *alerterHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var m pb.Metric
		consumedMessages.Inc()
		err := proto.Unmarshal(msg.Value, &m)
		if err != nil {
			log.Printf("Error unmarshaling message: %s\n", err)
			errorsMessages.WithLabelValues("decode").Inc()
			continue
		}

		for _, r := range Evaluate(&m) {
			if h.isSilenced(sess.Context(), m.GetHost(), r.Name) {
				continue
			}
			var alKey, rKey = alertKey(m.GetHost(), r.Name), renotifyKey(m.GetHost(), r.Name)
			isNewAlert, err := h.rdb.SetNX(sess.Context(), alKey, m.GetValue(), h.dedupTTL).Result()
			if err != nil {
				log.Printf("Error setting key: %s\n", err)
			}
			a := pb.Alert{Host: m.GetHost(), Rule: r.Name, Metric: m.GetName(), Value: m.GetValue(), Threshold: r.Threshold, State: pb.AlertState_ALERT_STATE_FIRING, Timestamp: m.GetTimestamp()}
			pubMsg, _ := proto.Marshal(proto.Message(&a))

			if isNewAlert {
				start := time.Now()
				_, _, err = h.producer.SendMessage(&sarama.ProducerMessage{Topic: pubTopic, Key: sarama.StringEncoder(m.GetHost()), Value: sarama.ByteEncoder(pubMsg)})
				if err != nil {
					log.Printf("Error sending message: %s\n", err)
					errorsMessages.WithLabelValues("send").Inc()
				}
				h.rdb.Set(sess.Context(), rKey, "1", h.renotifyTTL)
				flushDuration.Observe(time.Since(start).Seconds())
			} else {
				h.rdb.Expire(sess.Context(), alKey, h.dedupTTL)
				shouldRenotify, err := h.rdb.SetNX(sess.Context(), rKey, m.GetValue(), h.renotifyTTL).Result()
				if err != nil {
					log.Printf("Error setting key: %s\n", err)
					errorsMessages.WithLabelValues("setting").Inc()
				}
				if shouldRenotify {
					_, _, err = h.producer.SendMessage(&sarama.ProducerMessage{Topic: pubTopic, Key: sarama.StringEncoder(m.GetHost()), Value: sarama.ByteEncoder(pubMsg)})
					if err != nil {
						log.Printf("Error sending message: %s\n", err)
						errorsMessages.WithLabelValues("send").Inc()
					}
				}
			}
		}
		sess.MarkMessage(msg, "")
	}

	return nil
}

func (h *alerterHandler) isSilenced(ctx context.Context, host string, name string) bool {
	n, _ := h.rdb.Exists(ctx, SilenceKey(host, name), SilenceKey(host, "*")).Result()
	if n > 0 {
		return true
	}
	return false
}

func SilenceKey(host, rule string) string {
	return fmt.Sprintf("silence:%s:%s", host, rule)
}

func alertKey(host, name string) string {
	return fmt.Sprintf("alert:active:%s:%s", host, name)
}
func renotifyKey(host, name string) string {
	return fmt.Sprintf("alert:renotify:%s:%s", host, name)
}
