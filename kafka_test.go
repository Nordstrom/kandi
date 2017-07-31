package main

import (
	"testing"
	"fmt"
	"strings"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"time"
	"errors"
)

var EndOfTest = errors.New("EndOfTest")

type MockConsumer struct {
	pointIndex			int
	pointsToReturn 			[]string
	errorToReturn			error
	markedOffsets			[]*sarama.ConsumerMessage
	closed				bool
}

func (c *MockConsumer) Consume() (*sarama.ConsumerMessage,  *cluster.Notification, error) {
	if c.pointIndex < len(c.pointsToReturn) {
		i := c.pointIndex
		c.pointIndex += 1
		return &sarama.ConsumerMessage{Value: []byte(c.pointsToReturn[i]), Offset: int64(i), Timestamp: time.Now()}, nil, nil
	}
	return nil, nil, EndOfTest
}

func (c *MockConsumer) Close() () {
	c.closed = true
}

func (c *MockConsumer) MarkOffset(message *sarama.ConsumerMessage) {
	if c.markedOffsets != nil {
		c.markedOffsets = append(c.markedOffsets, message)
	} else {
		c.markedOffsets = []*sarama.ConsumerMessage{message}
	}
}

var TestIntegrationKafkaConfig = KafkaConfig{
	"kafka:9092",
	"test",
	"test",
	true,
	NewKafkaConfig().Cluster,
}

func seedPoints(points []string) (error) {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(strings.Split(TestIntegrationKafkaConfig.Brokers, ","), conf)
	if err != nil {
		return err
	}

	for _, point := range points {
		msg := &sarama.ProducerMessage{Topic: TestIntegrationKafkaConfig.Topics, Value: sarama.StringEncoder(point)}
		producer.SendMessage(msg)
	}
	return producer.Close()
}

func Test_Integration_Should_Seed_And_Consume_Kafka_Messages(t *testing.T) {
	if testing.Verbose() {
		expected := "ceph_pgmap_state,host=ceph-mon-0,state=active+clean count=10"
		err := seedPoints([]string{expected})
		if err != nil {
			t.Fatal(fmt.Sprintf("failed to seed points error: %s", err))
		}

		sut, _ := NewKafkaConsumer(&TestIntegrationKafkaConfig)
		defer sut.Consumer.Close()
		for {
			message, notification, err := sut.Consume()
			if err != nil {
				t.Fatal(fmt.Sprintf("Failed to recieve message: %s", err.Error()))
			} else if notification != nil {
				println("rebalance")
			} else {
				if message != nil {
					sut.MarkOffset(message)
					if string(message.Value) != expected {
						t.Fatal(fmt.Sprintf("Failed to recieve correct message. Expecting: %s, Actual: %s", expected, message.Value))
					} else {
						println("success")
					}
					break
				}
			}
		}

	}
}