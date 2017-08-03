package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type InfluxResponse struct {
	message string
	status  int
}

type Test_Kafka_Messages struct {
	pointsToReturn  []string
	expectedOffsets []string
	expectedSent    []string
}

type KandiTestSuite struct {
	label                           string
	influxHandler                   func(w http.ResponseWriter, r *http.Request)
	pointsToReturn                  []string
	expectedMessagesOffsetCommitted []string
	expectedMessagesSentToInflux    []string
	expectedErrorReturned           error
}

var KandiTestCases = []KandiTestSuite{
	{
		"Should Send and Commit Offset For Valid Points",
		func(w http.ResponseWriter, r *http.Request) {},
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"service.non-heap.usage,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=non-heap.usage value=-119670848.0 1501096898000000000",
		},
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"service.non-heap.usage,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=non-heap.usage value=-119670848.0 1501096898000000000",
		},
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"service.non-heap.usage,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=non-heap.usage value=-119670848.0 1501096898000000000",
		},
		EndOfTest,
	},
	{
		"Should Commit Offset But Not Send to Influx Points Encountering Parse Failures",
		func(w http.ResponseWriter, r *http.Request) {},
		[]string{"Will not be able to parse"},
		[]string{"Will not be able to parse"},
		[]string{},
		EndOfTest,
	},
}

func NewKandiTestConfig(url string, batchSize int) *Config {
	conf := &Config{Kandi: &KandiConfig{Batch: &Batch{}}, Influx: &InfluxConfig{}, Kafka: &KafkaConfig{}}
	conf.Influx.Database = "testdb"
	conf.Influx.Precision = ""
	conf.Influx.Url = url
	conf.Influx.RetentionPolicy = ""
	conf.Influx.WriteConsistency = "any"
	conf.Influx.BatchSize = 1
	conf.Influx.User = "user"
	conf.Influx.Password = "pass"
	conf.Influx.UserAgent = "agent"
	conf.Influx.Timeout = time.Duration(5) * time.Millisecond
	conf.Kandi.Batch.Size = batchSize
	return conf
}

func TestKandi(t *testing.T) {
	for _, testCase := range KandiTestCases {
		t.Run(testCase.label, func(t *testing.T) {

			influxHandler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				testCase.AssertBatchSentToInflux(t, w, r)
				testCase.influxHandler(w, r)
			}))
			defer influxHandler.Close()

			conf := NewKandiTestConfig(influxHandler.URL, len(testCase.pointsToReturn))
			influx := &Influx{conf.Influx}
			influx.config.Precision = "ns"
			sut := &Kandi{conf, NewMockConsumer(testCase.pointsToReturn), influx}
			sut.conf.Influx.Url = influxHandler.URL
			fmt.Println(influxHandler.URL)

			actual := sut.Start()

			testCase.AssertExpectedError(actual, t)
			testCase.AssertOffsetsCommitted(sut, t)
			testCase.AssertConsumerClosed(sut, t)
		})
	}
}

func (suite *KandiTestSuite) AssertExpectedError(actual error, t *testing.T) {
	if actual.Error() != suite.expectedErrorReturned.Error() {
		t.Fatal(fmt.Sprintf("%s: assert_response_error: actual: %s, expected: %s", suite.label, actual.Error(), suite.expectedErrorReturned.Error()))
	}
}

func (suite *KandiTestSuite) AssertConsumerClosed(sut *Kandi, t *testing.T) {
	if !sut.Consumer.(*MockConsumer).closed {
		t.Error(fmt.Sprintf("%s: assert_consumer_closed did not close the consumer properly", suite.label))
	}
}

func (suite *KandiTestSuite) AssertOffsetsCommitted(sut *Kandi, t *testing.T) {
	consumer := sut.Consumer.(*MockConsumer)
	visited := make(map[string]bool)
	for _, message := range consumer.markedOffsets {
		if message != nil {
			visited[string(message.Value)] = false
		}
	}
	for _, point := range suite.expectedMessagesOffsetCommitted {
		if _, ok := visited[point]; !ok {
			t.Error(fmt.Sprintf("%s: offsets committed did not commit message offset as expected.\n expected: %s\n", suite.label, point))
		}
	}
}

func (suite *KandiTestSuite) AssertBatchSentToInflux(t *testing.T, w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	bodyString := string(body)
	for _, actual := range strings.Split(bodyString, "\n") {
		if actual == "" {
			break
		}
		found := false
		for _, expected := range suite.expectedMessagesSentToInflux {
			if strings.Contains(actual, expected) {
				found = true
				break
			}
		}
		if !found {
			t.Error(fmt.Sprintf("%s: Message sent to Influx was not expected.\n\tactual: %s", suite.label, actual))
		}
	}
}
