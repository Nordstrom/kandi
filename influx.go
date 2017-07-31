package main

import (
	"time"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/prometheus/common/log"
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
}

type Influx struct {
	influxConfig		*InfluxConfig
	Client			Client
	Batch			influx.BatchPoints
}

func (c *Influx) Write(point *influx.Point) (error) {
	log.With("point", point.String()).Info("Adding point to batch points")
	if c.Batch == nil {
		log.With("database", c.influxConfig.Database).With("precision", c.influxConfig.Precision).Info("Creating new batch")
		bpConfig := influx.BatchPointsConfig{Precision: c.influxConfig.Precision, Database: c.influxConfig.Database, RetentionPolicy: c.influxConfig.RetentionPolicy, WriteConsistency: c.influxConfig.WriteConsistency}
		bp, err := influx.NewBatchPoints(bpConfig)
		if err != nil {
			log.With("error", err.Error()).With("database", c.influxConfig.Database).With("precision", c.influxConfig.Precision).Error("Creating new batch")
			return err
		}
		log.With("database", c.influxConfig.Database).With("precision", c.influxConfig.Precision).Info("Successfully created new batch")
		c.Batch = bp
	}
	c.Batch.AddPoint(point)
	if len(c.Batch.Points()) >= c.influxConfig.BatchSize {
		log.With("BatchedPoints", len(c.Batch.Points())).With("Url", c.influxConfig.Url).With("Database", c.influxConfig.Database).Info("Writing points to Influx")
		err := c.Client.Write(c.Batch)
		if err != nil {
			log.With("Error", err).With("BatchedPoints", len(c.Batch.Points())).With("Url", c.influxConfig.Url).With("Database", c.influxConfig.Database).Errorf("Writing points to Influx failed", err)
			return err
		}
		log.With("BatchedPoints", len(c.Batch.Points())).With("Url", c.influxConfig.Url).With("Database", c.influxConfig.Database).Info("Successfully wrote points to influx")
		c.Batch = nil
	}
	return nil
}

func (c *Influx) Ping() (time.Duration, string, error) {
	log.With("Url", c.influxConfig.Url).With("Database", c.influxConfig.Database).Info("Pinging Influx")
	duration, response, err := c.Client.Ping(c.influxConfig.Timeout)
	if err != nil {
		log.With("Url", c.influxConfig.Url).With("Database", c.influxConfig.Database).With("Error", err).Errorf("Failed to ping influx", err)
	}
	log.With("Url", c.influxConfig.Url).With("Database", c.influxConfig.Database).Info("Successfully pinged influx")
	return duration, response, err
}

func (c *Influx) Test() (*influx.Response, error) {
	log.With("Url", c.influxConfig.Url).With("Database", c.influxConfig.Database).With("Query", "SHOW SERIES").Info("Testing Influx Database")
	query := influx.NewQuery("SHOW SERIES", c.influxConfig.Database, c.influxConfig.Precision)
	response, err := c.Client.Query(query)
	if err != nil {
		log.With("Url", c.influxConfig.Url).With("Database", c.influxConfig.Database).With("Error", err).With("Query", "SHOW SERIES").Errorf("Failed to test influx database", err)
	}
	log.With("Url", c.influxConfig.Url).With("Database", c.influxConfig.Database).Info("Successfully tested influx database")
	return response, err
}

func NewInflux(config *InfluxConfig) (*Influx, error) {
	client, err := NewInfluxClient(config)
	if err != nil {
		return nil, err
	}
	return &Influx{influxConfig: config, Client: client}, nil
}