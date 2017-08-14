package main

import (
	"expvar"
	"time"
)

var MetricsInfluxWriteSuccess = expvar.NewInt("influxPointsWritten")
var MetricsKafkaMessages = expvar.NewInt("kafkaMessagesConsumed")

var MetricKafkaBackoff = expvar.NewInt("kafkaBackoffs")
var MetricInfluxBackoff = expvar.NewInt("influxBackoffs")
var MetricMainBackoff = expvar.NewInt("mainBackoffs")

var MetricsKafkaDuration = expvar.NewInt("kafkaConsumptionDuration")
var MetricsInfluxProcessDuration = expvar.NewInt("influxProcessDuration")

var MetricsKafkaInitializationFailure = expvar.NewInt("kafkaInitializationFailure")
var MetricInfluxInitializationFailure = expvar.NewInt("influxInitializationFailure")

var MetricsKafkaBatchDurationExceeded = expvar.NewInt("kafkaMaxDurationExceeded")
var MetricsKafkaConsumptionError = expvar.NewInt("kafkaConsumptionError")

var MetricsInfluxWriteFailure = expvar.NewInt("influxWriteFailure")
var MetricsInfluxParseFailure = expvar.NewInt("influxParseFailure")

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
