package main

import (
	"github.com/bsm/sarama-cluster"
	"strings"
	"github.com/Shopify/sarama"
	"github.com/prometheus/common/log"
)

type Offsets struct {

}

type KafkaConfig struct {
	Brokers		string
	Topics 		string
	ConsumerGroup 	string
	LoggingEnabled	bool
	Cluster 	*cluster.Config
}

type Consumer interface {
	Consume() (*sarama.ConsumerMessage,  *cluster.Notification, error)
	MarkOffset(message *sarama.ConsumerMessage)
	Close()
}

type KafkaConsumer struct {
	userConfig		*KafkaConfig
	Consumer		*cluster.Consumer
}

func NewKafkaConsumer(userConfig *KafkaConfig) (*KafkaConsumer, error) {
	brokers := strings.Split(userConfig.Brokers, ",")
	topics := strings.Split(userConfig.Topics, ",")

	log.With("brokers", userConfig.Brokers).With("topics", userConfig.Topics).Info("Creating new kafka consumer")
	consumer, err := cluster.NewConsumer(brokers, userConfig.ConsumerGroup, topics, getClusterConfiguration(userConfig))
	if err != nil {
		log.With("brokers", userConfig.Brokers).With("topics", userConfig.Topics).With("Error", err.Error()).Error("Error creating new kafka consumer")
		return nil, err
	}
	return &KafkaConsumer{userConfig, consumer}, nil
}

func (c *KafkaConsumer) Consume() (*sarama.ConsumerMessage,  *cluster.Notification, error) {

	select {
	case message, more := <-c.Consumer.Messages():
		if more {
			return message, nil, nil
		}
	case err, more := <-c.Consumer.Errors():
		if more {
			return nil, nil, err
		}
	case ntf, more := <-c.Consumer.Notifications():
		if more {
			return nil, ntf, nil
		}
	}
	return nil, nil, nil
}

func (c *KafkaConsumer) Close() {
	log.Debug("Closing consumer")
	c.Close()
}

func (c *KafkaConsumer) MarkOffset(message *sarama.ConsumerMessage) {
	log.With("offset", message.Offset).With("topic", message.Topic).With("partition", message.Partition).Info("Marking offset for message")
	c.Consumer.MarkOffset(message, "")
}

func getClusterConfiguration(userConfig *KafkaConfig) (*cluster.Config) {
	log.Info("Creating cluster configuration")
	conf := cluster.NewConfig()
	conf.Consumer.Return.Errors = true
	conf.Group.Return.Notifications = true
	return conf
}