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
	PostProcessors []func(processedMessages []*sarama.ConsumerMessage) bool
}

func NewKandi(conf *Config) *Kandi {
	influx := &Influx{conf.Influx}
	return &Kandi{conf: conf, Influx: influx, PostProcessors: []func(processedMessages []*sarama.ConsumerMessage) bool {}}
}

var MESSAGES_READY_TO_PROCESS chan []*sarama.ConsumerMessage
var PROCESSING_COMPLETED chan bool
var STOP_CONSUMING chan bool
var CONSUMING_COMPLETED chan bool

func (k *Kandi) Start() bool {
	STOP_CONSUMING = make(chan bool, 2)
	PROCESSING_COMPLETED = make(chan bool, 2)
	CONSUMING_COMPLETED = make(chan bool, 2)

	MESSAGES_READY_TO_PROCESS = make(chan []*sarama.ConsumerMessage, 5)

	go k.ConsumeMessages()
	go k.Process()

	doneProcessing := <-PROCESSING_COMPLETED
	log.WithField("doneProcessing", doneProcessing).Debug("Completed Processing")
	STOP_CONSUMING <-true
	doneConsuming := <-CONSUMING_COMPLETED
	log.WithField("doneConsuming", doneConsuming).Debug("Completed Consuming")
	defer close(PROCESSING_COMPLETED)
	defer close(MESSAGES_READY_TO_PROCESS)
	return true
}

func (k *Kandi) ConsumeMessages() {
	log.Debug("Starting to consume messages from kafka")
	backoff := NewBackoffHandler("kafka", k.conf)

	for {
		select {
		case _, ok := <-STOP_CONSUMING:
			if ok {
				log.Debug("Received stopping condition")
			} else {
				log.Debug("STOP_CONSUMING channel already closed")
			}
			log.Debug("Stopping consumer")
			if k.Consumer != nil {
				k.Consumer.Close()
			}
			CONSUMING_COMPLETED <- true
			return
		default:
			batchOfMessages, err := k.fromKafka()
			if batchOfMessages != nil && len(batchOfMessages) > 0 {
				MESSAGES_READY_TO_PROCESS <- batchOfMessages
			}
			if err != nil {
				backoff.Handle()
			}
		}
	}
}

func (k *Kandi) fromKafka() ([]*sarama.ConsumerMessage, error) {
	if k.Consumer == nil {
		consumer, err := NewKafkaConsumer(k.conf.Kafka)
		if err == nil {
			k.Consumer = consumer
		} else {
			return nil, err
		}
	}

	maxDuration := k.conf.Kandi.Batch.Duration

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
		} else if message != nil {
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
			stop, err := k.toInflux(batchOfMessages)
			if err != nil {
				backoff.Handle()
			} else if stop {
				PROCESSING_COMPLETED <- true
			} else {
				batchOfMessages = nil
			}
		}
	}
}

func (k *Kandi) toInflux(batchOfMessages []*sarama.ConsumerMessage) (bool, error) {
	startTime := time.Now()

	influxBatch, err := k.Influx.NewBatch()
	if err != nil {
		log.WithError(err).Error("Failed to create new influx batch")
		return false, err
	}

	for _, message := range batchOfMessages {
		point, err := k.Influx.ParseMessage(message)
		if err == nil && point != nil {
			influxBatch.AddPoint(point)
		}
	}

	err = k.Influx.Write(influxBatch)
	if err != nil {
		return false, err
	}

	k.Consumer.MarkOffset(batchOfMessages)
	MetricsInfluxProcessDuration.Add(time.Since(startTime).Nanoseconds())
	for _, processor := range k.PostProcessors {
		stop := processor(batchOfMessages)
		if stop {
			log.Debug("Post processing triggered processing to stop")
			return true, nil
		}
	}
	return false, nil
}