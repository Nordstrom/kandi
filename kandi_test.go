package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"github.com/Shopify/sarama"
	"errors"
	log "github.com/sirupsen/logrus"
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
	pointsToReturn                  []string
	expectedMessagesOffsetCommitted []string
	expectedMessagesSentToInflux    []string
	expectedErrorReturned           error
	batchSize						int
}

var KandiConsumerTestCases = []KandiTestSuite{
	{
		"Should consume messages without errors",
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"service.non-heap.usage,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=non-heap.usage value=-119670848.0 1501096898000000000",
			"service.non-heap.usage,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=non-heap.usage value=-119670848.0 1501096898000000001",
		},
		[]string{},
		[]string{},
		nil,
		3,
	},
	{
		"Should return error",
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"service.non-heap.usage,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=non-heap.usage value=-119670848.0 1501096898000000000",
		},
		[]string{},
		[]string{},
		errors.New("EndOfTest"),
		3,
	},
}

func NewKandiTestConfig(url string, batchSize int) *Config {
	conf := &Config{Kandi: &KandiConfig{Batch: &Batch{}, Backoff: &Backoff{}}, Influx: &InfluxConfig{}, Kafka: &KafkaConfig{}}
	conf.Influx.Database = "testdb"
	conf.Influx.Precision = ""
	conf.Influx.Url = url
	conf.Influx.RetentionPolicy = ""
	conf.Influx.WriteConsistency = "any"
	conf.Influx.User = "user"
	conf.Influx.Password = "pass"
	conf.Influx.UserAgent = "agent"
	conf.Influx.Timeout = time.Duration(1) * time.Millisecond
	conf.Kandi.Batch.Size = int64(batchSize)
	conf.Kandi.Batch.Duration = time.Duration(100) * time.Second
	conf.Kandi.Backoff.Interval = time.Duration(1) * time.Millisecond
	return conf
}

func TestKandi(t *testing.T) {
	for _, testCase := range KandiConsumerTestCases {
		t.Run(testCase.label, func(t *testing.T) {

			conf := NewKandiTestConfig("localhost:8086", testCase.batchSize)
			sut := NewKandi(conf)
			sut.Consumer = NewMockConsumer(testCase.pointsToReturn)
			log.SetLevel(log.PanicLevel)
			actual, err := sut.fromKafka()
			testCase.AssertErrorReturned(err, sut, t)
			testCase.AssertMessagesBatched(actual, sut, t)
		})
	}
}

func (suite *KandiTestSuite) AssertMessagesBatched(actual []*sarama.ConsumerMessage, sut *Kandi, t *testing.T) {
	if (actual == nil || len(actual) == 0) && len(suite.pointsToReturn) > 0 {
		t.Error(fmt.Sprintf("%s: Did not receive any messages when expecting %d", suite.label, len(suite.pointsToReturn)))
	}
	for _, expected := range suite.pointsToReturn {
		found := false
		for _, acutalPoint := range actual {

			if acutalPoint != nil && string(acutalPoint.Value) == expected {
				found = true
				break
			}
		}
		if !found {
			t.Error(fmt.Sprintf("%s: Expecing message was not found in returned messages.\n\texpecting:%s", suite.label, expected))
		}
	}
}

func (suite *KandiTestSuite) AssertConsumerClosed(sut *Kandi, t *testing.T) {
	if !sut.Consumer.(*MockConsumer).closed {
		t.Error(fmt.Sprintf("%s: assert_consumer_closed did not close the consumer properly", suite.label))
	}
}



var KandiProcessInvalidResponseTestCases = []KandiTestSuite{
	{
		"Should return error when influx responses with an error",
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
		errors.New("A Test Error"),
		2,
	},
}

func Test_Should_Process_Messages_To_Influx_With_Error_Response(t *testing.T) {

	for _, testCase := range KandiProcessInvalidResponseTestCases {
		t.Run(testCase.label, func(t *testing.T) {

			influxHandler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(500)
				w.Write([]byte(testCase.expectedErrorReturned.Error()))
			}))
			defer influxHandler.Close()

			input := []*sarama.ConsumerMessage{}
			for i, point := range testCase.pointsToReturn {
				input = append(input, &sarama.ConsumerMessage{Value: []byte(point), Offset: int64(i), Timestamp: time.Now()})
			}

			conf := NewKandiTestConfig(influxHandler.URL, len(testCase.pointsToReturn))
			sut := NewKandi(conf)
			sut.Consumer = NewMockConsumer(testCase.pointsToReturn)
			log.SetLevel(log.PanicLevel)

			actual := sut.toInflux(input)

			testCase.AssertErrorReturned(actual, sut, t)
		})
	}
}

var KandiProcessValidRepsonseTestCases = []KandiTestSuite{
	{
		"Should Send and Commit Offset For Valid Points",
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
		nil,
		2,
	},
	{
		"Should Commit Offset But Not Send to Influx Points Encountering Parse Failures",
		[]string{"Will not be able to parse"},
		[]string{"Will not be able to parse"},
		[]string{},
		nil,
		1,
	},
}

func Test_Should_Process_Messages_To_Influx_With_Valid_Response(t *testing.T) {

	for _, testCase := range KandiProcessValidRepsonseTestCases {
		t.Run(testCase.label, func(t *testing.T) {

			influxHandler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				testCase.AssertBatchSentToInflux(t, w, r)
			}))
			defer influxHandler.Close()

			input := []*sarama.ConsumerMessage{}
			for i, point := range testCase.pointsToReturn {
				input = append(input, &sarama.ConsumerMessage{Value: []byte(point), Offset: int64(i), Timestamp: time.Now()})
			}

			conf := NewKandiTestConfig(influxHandler.URL, len(testCase.pointsToReturn))
			sut := NewKandi(conf)
			sut.Consumer = NewMockConsumer(testCase.pointsToReturn)
			log.SetLevel(log.PanicLevel)

			actual := sut.toInflux(input)

			testCase.AssertErrorReturned(actual, sut, t)
			testCase.AssertOffsetsCommitted(sut, t)
		})
	}
}

func (suite *KandiTestSuite) AssertErrorReturned(actual error, sut *Kandi, t *testing.T) {
	if actual == nil && suite.expectedErrorReturned == nil {
		return
	}
	if actual == nil && suite.expectedErrorReturned != nil {
		t.Error(fmt.Sprintf("%s: Did not encounter error when expecing to.\n\texpecting: %s", suite.label, suite.expectedErrorReturned.Error()))
	} else if actual != nil && suite.expectedErrorReturned == nil {
		t.Error(fmt.Sprintf("%s: Unexpected error when processing message to influx, expecting none.\n\tactual: %s", suite.label, actual.Error()))
	} else if actual.Error() != suite.expectedErrorReturned.Error() {
		t.Error(fmt.Sprintf("%s: Unexpected error when processing message to influx.\n\texpected: %s\n\tactual: %s", suite.label, suite.expectedErrorReturned.Error(), actual.Error()))
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