package kafka

import (
	"github.com/IBM/sarama"
)

func NewProducer(addr string) (sarama.SyncProducer, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V3_6_0_0

	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Idempotent = true
	conf.Producer.Return.Successes = true
	conf.Producer.Retry.Max = 5
	conf.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer([]string{addr}, conf)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func PublishMetric(p sarama.SyncProducer, topic, host string, value []byte) error {
	msg := &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(host), Value: sarama.ByteEncoder(value)}
	_, _, err := p.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}
