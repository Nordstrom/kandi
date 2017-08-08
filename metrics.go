package main

import (
	"expvar"
	"time"
)

var MetricKafkaBackoff = expvar.NewInt("kafkaBackoffs")
var MetricInfluxBackoff = expvar.NewInt("influxBackoffs")
var MetricMainBackoff = expvar.NewInt("mainBackoffs")
var MetricsInfluxWriteSuccess = expvar.NewInt("pointsWritten")
var MetricPointsParseFailure = expvar.NewInt("pointsParseFailure")
var MetricWriteAttemptFailure = expvar.NewInt("writeAttemptFailure")
var MetricInfluxInitializationFailure = expvar.NewInt("influxInitializationFailure")
var MetricsKafkaInitializationFailure = expvar.NewInt("kafkaInitializationFailure")
var MetricFieldTypeConflict = expvar.NewInt("fieldTypeConflict")
var MetricPartialWrite = expvar.NewInt("partialWrite")
var MetricsKafkaBatchDurationExceeded = expvar.NewInt("kafkaBatchDurationExceeded")
var MetricsKafkaConsumptionError = expvar.NewInt("kafkaConsumptionError")
var MetricsKafkaMessages = expvar.NewInt("kafkaMessagesConsumed")
var MetricsKafkaDuration = expvar.NewInt("kafkaDuration")
var MetricsInfluxWriteFailure = expvar.NewInt("influxWriteFailure")
var MetricsInfluxParseFailure = expvar.NewInt("influxParseFailure")
var MetricsInfluxDuration = expvar.NewInt("influxWriteDuration")


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