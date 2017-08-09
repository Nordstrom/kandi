package main

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"time"
)

type Kandi struct {
	conf     *Config
	Consumer Consumer
	Influx   *Influx
}

func NewKandi(conf *Config) *Kandi {
	influx := &Influx{conf.Influx}
	return &Kandi{conf: conf, Influx: influx}
}

var MESSAGES_READY_TO_PROCESS chan []*sarama.ConsumerMessage
var DONE_NOTIFICATIONS chan bool

func (k *Kandi) Start() bool {
	DONE_NOTIFICATIONS = make(chan bool, 10)
	MESSAGES_READY_TO_PROCESS = make(chan []*sarama.ConsumerMessage, 100)

	go k.ConsumeMessages()
	go k.Process()
	isDone := <-DONE_NOTIFICATIONS

	defer close(DONE_NOTIFICATIONS)
	defer close(MESSAGES_READY_TO_PROCESS)
	if k.Consumer != nil {
	k.Consumer.Close()
	}
	
	return isDone
}

func (k *Kandi) ConsumeMessages() {
	log.Debug("Starting to consume messages from kafka")
	backoff := NewBackoffHandler("kafka", k.conf)

	for {
		batchOfMessages, err := k.fromKafka()
		if batchOfMessages != nil && len(batchOfMessages) > 0 {
			MESSAGES_READY_TO_PROCESS <- batchOfMessages
		}
		if err != nil {
			backoff.Handle()
		}
	}
}

func (k *Kandi) fromKafka() ([]*sarama.ConsumerMessage, error) {
	maxDuration := k.conf.Kandi.Batch.Duration

	if k.Consumer == nil {
		consumer, err := NewKafkaConsumer(k.conf.Kafka)
		if err == nil {
			k.Consumer = consumer
		} else {
			return nil, err
		}
	}

	consumedMessages := make([]*sarama.ConsumerMessage, k.conf.Kandi.Batch.Size)
	startTime := time.Now()

	var batched int64
	for batched = 0; batched < k.conf.Kandi.Batch.Size; batched++ {

		if time.Since(startTime) > maxDuration {
			MetricsKafkaBatchDurationExceeded.Add(1)
			break
		}

		message, err := k.Consumer.ConsumeMessage()
		if err != nil {
			log.WithError(err).Error("Kafka encountered an error while consuming")
			MetricsKafkaConsumptionError.Add(1)
			return consumedMessages, err
		} else {
			consumedMessages = append(consumedMessages, message)
		}
	}
	MetricsKafkaConsumption(startTime, batched)
	return consumedMessages, nil
}

func (k *Kandi) Process() {
	log.Debug("Starting to process messages")
	backoff := NewBackoffHandler("influx", k.conf)

	var batchOfMessages []*sarama.ConsumerMessage

	for {
		if batchOfMessages == nil {
			messagesFromKafka, ok := <-MESSAGES_READY_TO_PROCESS
			if ok {
				batchOfMessages = messagesFromKafka
			}
		} else {
			err := k.toInflux(batchOfMessages)
			if err != nil {
				backoff.Handle()
			} else {
				batchOfMessages = nil
			}
		}
	}
}

func (k *Kandi) toInflux(batchOfMessages []*sarama.ConsumerMessage) error {
	startTime := time.Now()

	influxBatch, err := k.Influx.NewBatch()
	if err != nil {
		log.WithError(err).Error("Failed to create new influx batch")
		return err
	}

	for _, message := range batchOfMessages {
		point, err := k.Influx.ParseMessage(message)
		if err == nil && point != nil {
			influxBatch.AddPoint(point)
		}
	}

	err = k.Influx.Write(influxBatch)
	if err != nil {
		return err
	}

	k.Consumer.MarkOffset(batchOfMessages)
	MetricsInfluxProcessDuration.Add(time.Since(startTime).Nanoseconds())
	return nil
}