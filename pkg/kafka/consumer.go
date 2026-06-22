package kafka

import "github.com/IBM/sarama"

func NewConsumerGroup(addr, groupId string) (sarama.ConsumerGroup, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V3_6_0_0
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Consumer.Return.Errors = true

	cg, err := sarama.NewConsumerGroup([]string{addr}, groupId, conf)

	return cg, err
}
