package main

import (
	"testing"
	"net/http/httptest"
	"net/http"
	influx "github.com/influxdata/influxdb/client/v2"
	"fmt"
	"strings"
)

type InfluxTestSuite struct {
	label 					string
	configuration			*InfluxConfig
	influxHandler			func(w http.ResponseWriter, r *http.Request)
	expectedError			string
}

func (suite *InfluxTestSuite) AssertErrorNotReceived(actual error, t *testing.T) {
	if actual == nil && suite.expectedError !=  "" {
		t.Error(fmt.Sprintf("%s: Expected error was not recieved.\n\texpected: %s", suite.label, suite.expectedError ))
	}
}

func (suite *InfluxTestSuite) AssertErrorNotExpected(actual error, t *testing.T) {
	if actual != nil && suite.expectedError == "" {
		t.Error(fmt.Sprintf("%s: Recieved error that should have not been received.\n\tactual %s", suite.label, actual.Error() ))
	}
}

func (suite *InfluxTestSuite) AssertUnexpected(actual error, t *testing.T) {
	if actual != nil && !strings.Contains(actual.Error(), suite.expectedError) {
		t.Error(fmt.Sprintf("%s: Recieved unexpected error.\n\texpected: %s\n\tactual %s", suite.label, suite.expectedError, actual.Error() ))
	}
}

var InfluxTestsCases = []InfluxTestSuite{
	{
		"Should Successfully Write Point To Influx Without Error",
		&InfluxConfig{},
		func(w http.ResponseWriter, r *http.Request){},
		"",
	},
	{
		"Should Successfully Write Point To Influx Without Error When Encountering A Field Type Conflict",
		&InfluxConfig{},
		func(w http.ResponseWriter, r *http.Request){
			w.WriteHeader(400)
			w.Write([]byte("{\"error\":\"write failed: field type conflict: input field \\\"value\\\" on measurement \\\"writingValidPoint\\\" is type string, already exists as type integer dropped=1\"}\n"))
		},
		"",
	},
	{
		"Should Successfully Write Point to Influx When Encountering a Partial Write Error",
		&InfluxConfig{},
		func(w http.ResponseWriter, r *http.Request){
			w.WriteHeader(400)
			w.Write([]byte("partial write"))
		},
		"",
	},
	{
		"Should Return Error When Encountering Authentication Error",
		&InfluxConfig{},
		func(w http.ResponseWriter, r *http.Request){
			w.WriteHeader(401)
			w.Write([]byte("unable to parse authentication credentials"))
		},
		"unable to parse authentication credentials",
	},
	{
		"Should Return Error Database Not Found",
		&InfluxConfig{},
		func(w http.ResponseWriter, r *http.Request){
			w.WriteHeader(404)
			w.Write([]byte("database not found"))
		},
		"database not found",
	},
}

func Test_Influx_With_Valid_Configuration(t *testing.T) {

	for _, testCase := range InfluxTestsCases {

		t.Run(testCase.label, func(t *testing.T){
			influxSpy := httptest.NewServer(http.HandlerFunc(testCase.influxHandler))
			defer influxSpy.Close()
			testCase.configuration.Url = influxSpy.URL
			testCase.configuration.AcceptedErrors = []string{"write failed: field type conflict: input", "partial write"}
			input, _ := influx.NewBatchPoints(influx.BatchPointsConfig{testCase.configuration.Precision, testCase.configuration.Database, testCase.configuration.RetentionPolicy, testCase.configuration.WriteConsistency})
			sut := &Influx{testCase.configuration}

			actual := sut.Write(input)

			testCase.AssertErrorNotReceived(actual, t)
			testCase.AssertErrorNotExpected(actual, t)
			testCase.AssertUnexpected(actual, t)
		})
	}
}