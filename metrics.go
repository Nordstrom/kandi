package main

import (
	"expvar"
)

var MetricBackoff = expvar.NewInt("backoffs")
var MetricPointsWritten = expvar.NewInt("pointsWritten")
var MetricPointsParseFailure = expvar.NewInt("pointsParseFailure")
var MetricWriteAttemptFailure = expvar.NewInt("writeAttemptFailure")
var MetricInfluxInitializationFailure = expvar.NewInt("influxInitializationFailure")
var MetricsKafkaInitializationFailure = expvar.NewInt("kafkaInitializationFailure")
var MetricFieldTypeConflict = expvar.NewInt("fieldTypeConflict")
var MetricPartialWrite = expvar.NewInt("partialWrite")
var MetricBatchDurationTaken = expvar.NewInt("batchDurationTakenNanoSec")