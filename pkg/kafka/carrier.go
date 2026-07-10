package kafka

import (
	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/propagation"
)

type ProducerCarrier struct {
	msg *sarama.ProducerMessage
}

type ConsumerCarrier struct {
	msg *sarama.ConsumerMessage
}

var _ propagation.TextMapCarrier = (*ProducerCarrier)(nil)
var _ propagation.TextMapCarrier = (*ConsumerCarrier)(nil)

func NewProducerCarrier(msg *sarama.ProducerMessage) *ProducerCarrier {
	return &ProducerCarrier{msg: msg}
}

func NewConsumerCarrier(msg *sarama.ConsumerMessage) *ConsumerCarrier {
	return &ConsumerCarrier{msg: msg}
}

func (pc *ProducerCarrier) Get(key string) string {
	for _, v := range pc.msg.Headers {
		if string(v.Key) == key {
			return string(v.Value)
		}
	}

	return ""
}

func (pc *ProducerCarrier) Set(key string, value string) {
	pc.msg.Headers = append(pc.msg.Headers, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	})
}
func (pc *ProducerCarrier) Keys() []string {
	res := make([]string, len(pc.msg.Headers))
	for i, h := range pc.msg.Headers {
		res[i] = string(h.Key)
	}
	return res
}

func (cc *ConsumerCarrier) Get(key string) string {
	for _, v := range cc.msg.Headers {
		if string(v.Key) == key {
			return string(v.Value)
		}
	}
	return ""
}

func (cc *ConsumerCarrier) Set(key, value string) {
	cc.msg.Headers = append(cc.msg.Headers, &sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	})
}

func (cc *ConsumerCarrier) Keys() []string {
	res := make([]string, len(cc.msg.Headers))
	for i, h := range cc.msg.Headers {
		res[i] = string(h.Key)
	}
	return res
}
