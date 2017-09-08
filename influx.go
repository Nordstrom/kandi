package main

import (
	"github.com/Shopify/sarama"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

type InfluxConfig struct {
	Url              string
	User             string
	Password         string
	Timeout          time.Duration
	UserAgent        string
	Database         string
	Precision        string
	BatchSize        int
	WriteConsistency string
	RetentionPolicy  string
	AcceptedErrors   []string
}

type Influx struct {
	config *InfluxConfig
}

func (i *Influx) Write(batch influx.BatchPoints) error {
	if batch != nil && len(batch.Points()) >= 0 {
		client, err := i.NewClient()
		defer client.Close()
		if err != nil {
			MetricInfluxInitializationFailure.Add(1)
			return err
		}
		err = client.Write(batch)
		if err != nil {
			if strings.Contains(err.Error(), "partial write") {
				MetricInfluxPartialWrite.Add(1)
				return nil
			} else if strings.Contains(err.Error(), "field type conflict") {
				MetricInfluxFieldTypeConflict.Add(1)
				return nil
			}
			log.WithError(err).WithField("points", len(batch.Points())).Error("Error while writing points")
			MetricsInfluxWriteFailure.Add(1)
			return err
		}
		MetricsInfluxWriteSuccess.Add(int64(len(batch.Points())))
	}
	return nil
}

func (i *Influx) NewClient() (influx.Client, error) {
	return influx.NewHTTPClient(influx.HTTPConfig{i.config.Url, i.config.User, i.config.Password, i.config.UserAgent, i.config.Timeout, false, nil})
}

func (i *Influx) ParseMessages(messages []*sarama.ConsumerMessage) influx.BatchPoints {
	batchConf := influx.BatchPointsConfig{i.config.Precision, i.config.Database, i.config.RetentionPolicy, i.config.WriteConsistency}
	batch, err := influx.NewBatchPoints(batchConf)
	if err != nil {
		log.WithFields(log.Fields{"precision": i.config.Precision, "database": i.config.Database, "retentionPolicy": i.config.RetentionPolicy, "WriteConsistency": i.config.WriteConsistency}).Info("Failed to create batch points configuration")
		return nil
	}

	if messages == nil || len(messages) == 0 {
		return batch
	}

	log.WithField("points", len(messages)).Debug("Parsing messages into points")
	for _, message := range messages {
		if message != nil && len(message.Value) != 0 {
			point, _ := i.ParseMessage(message)
			if point != nil {
				batch.AddPoint(point)
			}
		}
	}
	log.WithField("points", len(batch.Points())).Debug("Successfully parsed messages into points")
	return batch
}

func (i *Influx) ParseMessage(message *sarama.ConsumerMessage) (*influx.Point, error) {
	if message != nil && message.Value != nil {
		point, err := models.ParsePointsWithPrecision(message.Value, time.Now().UTC(), i.config.Precision)
		if err != nil || len(point) == 0 {
			log.WithError(err).Debug("Failed to parse message")
			MetricsInfluxParseFailure.Add(1)
			return nil, err
		}
		return influx.NewPointFrom(point[0]), nil
	}
	return nil, nil
}

func (i *Influx) NewBatch() (influx.BatchPoints, error) {
	batchConf := influx.BatchPointsConfig{i.config.Precision, i.config.Database, i.config.RetentionPolicy, i.config.WriteConsistency}
	batch, err := influx.NewBatchPoints(batchConf)
	if err != nil {
		log.WithFields(log.Fields{"precision": i.config.Precision, "database": i.config.Database, "retentionPolicy": i.config.RetentionPolicy, "WriteConsistency": i.config.WriteConsistency}).Info("Failed to create batch points configuration")
		return nil, err
	}
	return batch, nil
}
