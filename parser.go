package main

import (
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/prometheus/common/log"
)

func NewParser() Parser {
	log.Info("Creating Parser")
	return InfluxParser{}
}

type InfluxParser struct {

}

type ParsedMessage struct {
	Point		*influx.Point
	Err		error
}

func (p InfluxParser) parse(point []byte) (*ParsedMessage) {
	log.With("message", string(point)).Info("Parsing message into point")
	points, err := models.ParsePoints(point)
	if err != nil {
		log.With("message", string(point)).Error("Failed to parsed message into point")
		return &ParsedMessage{nil, err}
	}
	log.With("message", string(point)).Info("Successfully parsed message into point")
	return &ParsedMessage{influx.NewPointFrom(points[0]), nil}
}

type Parser interface {
	parse(points []byte) (*ParsedMessage)
}