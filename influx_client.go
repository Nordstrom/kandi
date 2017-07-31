package main

import (
	"time"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/prometheus/common/log"
)

type Client interface {
	Query(query influx.Query) (*influx.Response, error)
	Ping(time time.Duration) (time.Duration, string, error)
	Write(batch influx.BatchPoints) (error)
}

type InfluxClient struct {
	Client		Client
}

func (c InfluxClient) Write(batch influx.BatchPoints) (error) {
	return c.Client.Write(batch)
}

func (c InfluxClient) Ping(time time.Duration) (time.Duration, string, error) {
	return c.Client.Ping(time)
}

func (c InfluxClient) Query(query influx.Query) (*influx.Response, error) {
	return c.Client.Query(query)
}

func NewInfluxClient(config *InfluxConfig) (InfluxClient, error) {
	log.With("Url", config.Url).With("Database", config.Database).Info("Creating new influx http client")
	httpConfig := influx.HTTPConfig{config.Url, config.User, config.Password, config.UserAgent, config.Timeout, false, nil}
	client, err := influx.NewHTTPClient(httpConfig)
	if err != nil {
		log.With("Url", config.Url).With("Database", config.Database).Errorf("Failed to create new influx http client", err)
	}
	log.With("Url", config.Url).With("Database", config.Database).Info("Successfully created influx client")
	return InfluxClient{Client: client}, err
}