package main

import (
	"github.com/Shopify/sarama"
	influx "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
	"time"
)

type Kandi struct {
	conf     *Config
	Consumer Consumer
	Influx   *Influx
}

func (k *Kandi) initialize() error {
	if k.Consumer == nil {
		consumer, err := NewKafkaConsumer(k.conf.Kafka)
		if err != nil {
			MetricsKafkaInitializationFailure.Add(1)
			return err
		}
		k.Consumer = consumer
	}
	k.Influx = &Influx{k.conf.Influx}
	return nil
}

func (k *Kandi) Start() error {
	err := k.initialize()
	if err != nil {
		return err
	}
	defer k.Consumer.Close()
	for {
		start := time.Now()
		log.Debug("Consuming Messages")
		batch, messages, err := k.consumeMessages()
		if err != nil {
			MetricsKafkaConsumptionError.Add(1)
			return err
		}
		log.Debug("Writing batch")
		writeErr := k.Influx.Write(batch)
		if writeErr != nil {
			return writeErr
		}
		k.Consumer.MarkOffset(messages)
		MetricBatchDurationTaken.Set(time.Since(start).Nanoseconds())
	}
	log.Debug("Exiting Kandi Start")

	return nil
}

func (k *Kandi) consumeMessages() (influx.BatchPoints, []*sarama.ConsumerMessage, error) {
	maxBatchSize := k.conf.Kandi.Batch.Size
	maxDuration := k.conf.Kandi.Batch.Duration
	start := time.Now()
	messages := make([]*sarama.ConsumerMessage, maxBatchSize)
	batch, err := k.Influx.NewBatch()
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < maxBatchSize; i++ {

		message, err := k.Consumer.Consume()
		if err != nil {
			return nil, nil, err
		}

		if message != nil && message.Value != nil && len(message.Value) > 0 {
			messages = append(messages, message)
			parsed, _ := k.Influx.ParseMessage(message)
			if parsed != nil {
				batch.AddPoint(parsed)
			}
		}

		if maxDuration > 0 && time.Since(start) >= maxDuration {
			break
		}
	}
	return batch, messages, nil
}
