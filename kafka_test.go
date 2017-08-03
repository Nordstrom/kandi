package main

import (
	"github.com/Shopify/sarama"
	"time"
	"errors"
	"testing"
)

var EndOfTest = errors.New("EndOfTest")

type MockConsumer struct {
	pointReturnedIndex				int
	pointsToReturn 					[]string
	markedOffsets					[]*sarama.ConsumerMessage
	closed							bool
}

func NewMockConsumer(pointsToReturn []string) (*MockConsumer) {
	return &MockConsumer{0, pointsToReturn, make([]*sarama.ConsumerMessage, 1), false}
}

func (c *MockConsumer) Consume() (*sarama.ConsumerMessage,  error) {
	if c.pointReturnedIndex < len(c.pointsToReturn) {
		messagee := &sarama.ConsumerMessage{Value: []byte(c.pointsToReturn[c.pointReturnedIndex]), Offset: int64(c.pointReturnedIndex), Timestamp: time.Now()}
		c.pointReturnedIndex += 1
		return messagee, nil
	}
	return nil, EndOfTest
}

func (c *MockConsumer) Close() () {
	c.closed = true
}

func (c *MockConsumer) MarkOffset(messages []*sarama.ConsumerMessage) {
	for _, message := range messages {
		if c.markedOffsets != nil {
			c.markedOffsets = append(c.markedOffsets, message)
		} else {
			c.markedOffsets = make([]*sarama.ConsumerMessage, len(c.pointsToReturn))
		}
	}
}

func Test_(t *testing.T) {

}
