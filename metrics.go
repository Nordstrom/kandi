package main

import (
	"expvar"
	"time"
)

var MetricKafkaBackoff = expvar.NewInt("kafkaBackoffs")
var MetricInfluxBackoff = expvar.NewInt("influxBackoffs")
var MetricMainBackoff = expvar.NewInt("mainBackoffs")

var MetricsKafkaInitializationFailure = expvar.NewInt("kafkaInitializationFailure")
var MetricsKafkaBatchDurationExceeded = expvar.NewInt("kafkaMaxDurationExceeded")
var MetricsKafkaConsumptionError = expvar.NewInt("kafkaConsumptionError")
var MetricsKafkaMessages = expvar.NewInt("kafkaMessagesConsumed")
var MetricsKafkaDuration = expvar.NewInt("kafkaConsumptionDuration")

var MetricInfluxInitializationFailure = expvar.NewInt("influxInitializationFailure")
var MetricsInfluxWriteFailure = expvar.NewInt("influxWriteFailure")
var MetricsInfluxParseFailure = expvar.NewInt("influxParseFailure")
var MetricsInfluxProcessDuration = expvar.NewInt("influxProcessDuration")
var MetricsInfluxWriteSuccess = expvar.NewInt("influxPointsWritten")
var MetricInfluxPartialWrite = expvar.NewInt("influxPartialWrite")
var MetricInfluxFieldTypeConflict = expvar.NewInt("influxFieldTypeConflict")

func MetricsKafkaConsumption(startTime time.Time, points int64) {
	MetricsKafkaMessages.Add(points)
	MetricsKafkaDuration.Add(time.Since(startTime).Nanoseconds())
}

func MetricsBackoff(name string) {
	switch name {
	case "kafka":
		MetricKafkaBackoff.Add(1)
		break
	case "influx":
		MetricInfluxBackoff.Add(1)
		break
	case "main":
		MetricMainBackoff.Add(1)
		break
	}
}