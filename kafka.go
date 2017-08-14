package main

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
	"strings"
)

type KafkaConfig struct {
	Brokers        string
	Topics         string
	ConsumerGroup  string
	LoggingEnabled bool
	Cluster        *cluster.Config
}

type Consumer interface {
	MarkOffset(message []*sarama.ConsumerMessage)
	ConsumeMessage() (*sarama.ConsumerMessage, error)
	Close()
}

type KafkaConsumer struct {
	userConfig *KafkaConfig
	Consumer   *cluster.Consumer
}

func NewKafkaConsumer(userConfig *KafkaConfig) (*KafkaConsumer, error) {
	brokers := strings.Split(userConfig.Brokers, ",")
	topics := strings.Split(userConfig.Topics, ",")

	log.WithFields(log.Fields{"brokers": userConfig.Brokers, "topics": userConfig.Topics}).Debug("Creating new kafka consumer")
	consumer, err := cluster.NewConsumer(brokers, userConfig.ConsumerGroup, topics, userConfig.Cluster)
	if err != nil {
		log.WithFields(log.Fields{"brokers": userConfig.Brokers, "topics": userConfig.Topics}).WithError(err).Error("Error creating new kafka consumer")
		MetricsKafkaInitializationFailure.Add(1)
		return nil, err
	}
	return &KafkaConsumer{userConfig, consumer}, nil
}

func (c *KafkaConsumer) ConsumeMessage() (*sarama.ConsumerMessage, error) {
	select {
	case msg, more := <-c.Consumer.Messages():
		if more {
			return msg, nil
		}
	case err, more := <-c.Consumer.Errors():
		if more {
			return nil, err
		}
	case ntf, more := <-c.Consumer.Notifications():
		if more {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}
	return nil, nil
}

func (c *KafkaConsumer) Close() {
	log.Debug("Closing consumer")
	c.Consumer.Close()
}

func (c *KafkaConsumer) MarkOffset(messages []*sarama.ConsumerMessage) {
	for _, message := range messages {
		if message != nil {
			c.Consumer.MarkOffset(message, "")
		}
	}
}
