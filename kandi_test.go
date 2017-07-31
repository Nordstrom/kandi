package main

import (
	"fmt"
	"testing"
)

var IntegrationTestInfluxConfig = []byte(`
Brokers: localhost:9082
influx:
  Url: http://localhost:8082
  User: fred
  Password: fredspassword
  Timeout: 1
  UserAgent: meh
  Database: test-db
  Precision: test-precision
`)

type KandiTests struct {
		label				string
		input 				[]string
		expectedSent			[]string
		expectedCommitted		[]string
		expectedError			error
		batchSize			int
}

var Kandi_Consume_Tests = []KandiTests{
	{
		"When Processing Valid Points",
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"service.non-heap.usage,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=non-heap.usage value=-119670848.0 1501096898000000000",
			"service.counter.status.400.api.v4.styles,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=counter.status.400.api.v4.styles count=1.0 1501096898000000000",
		},
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"service.non-heap.usage,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=non-heap.usage value=-119670848.0 1501096898000000000",
			"service.counter.status.400.api.v4.styles,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=counter.status.400.api.v4.styles count=1.0 1501096898000000000",
		},
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"service.non-heap.usage,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=non-heap.usage value=-119670848.0 1501096898000000000",
			"service.counter.status.400.api.v4.styles,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=counter.status.400.api.v4.styles count=1.0 1501096898000000000",
		},
		EndOfTest,
		3,
	},
	{
		"When Processing An Invalid Point",
		[]string{
			"fake",
		},
		[]string{},
		[]string{
			"fake",
		},
		EndOfTest,
		1,
	},
	{
		"When Processing An Invalid Point Between Two Valid Points",
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"fake",
			"service.counter.status.400.api.v4.styles,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=counter.status.400.api.v4.styles count=1.0 1501096898000000000",
		},
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"service.counter.status.400.api.v4.styles,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=counter.status.400.api.v4.styles count=1.0 1501096898000000000",
		},
		[]string{
			"service.pools.PS-Eden-Space.used,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=pools.PS-Eden-Space.used value=308367096.0 1501096898000000000",
			"fake",
			"service.counter.status.400.api.v4.styles,app.type=product.service,build=802-master,cluster=undefined,data.center=undefined,host=172.22.78.51,metricName=counter.status.400.api.v4.styles count=1.0 1501096898000000000",
		},
		EndOfTest,
		2,
	},
}

func assert_committed_offsets(sut *KandiApp, test KandiTests, t *testing.T) {
	visited := make(map[string]bool)
	for _, point := range test.expectedCommitted {
		visited[point] = false
	}
	consumer := sut.Consumer.(*MockConsumer)
	if len(test.expectedCommitted) > len(consumer.markedOffsets) {
		t.Error(fmt.Sprintf("%s: assert_offsets_are_committed did not commit the proper number of offsets \n expected: %s, actual: %s\n", test.label, len(test.expectedCommitted), len(consumer.markedOffsets)))
	}
	for _, message := range consumer.markedOffsets {
		if _, ok := visited[string(message.Value)]; !ok {
			t.Error(fmt.Sprintf("%s: assert_offsets_are_committed did not commit message offset \n expected: %s\n", test.label, string(message.Value)))
		}
	}
}

func assert_consumer_closed(sut *KandiApp, test KandiTests, t *testing.T) {
	if !sut.Consumer.(*MockConsumer).closed {
		t.Error(fmt.Sprintf("%s: assert_consumer_closed did not close the consumer properly", test.label))
	}
}

func assert_written_to_influx(sut *KandiApp, test KandiTests, t *testing.T) {
	actual := sut.Influx.Client.(*MockClient).sent
	if len(actual) == 0 && len(test.expectedSent) > 0 {
		t.Error(fmt.Sprintf("%s: assert_points_sent_to_influx: Points, expected: %v, actual: %v", test.label, len(test.expectedSent), len(actual)))
	} else if len(actual) > 0 {
		visited := make(map[string]bool)
		for _, expectedPoint := range test.expectedSent {
			visited[expectedPoint] = false
		}
		for _, batch := range actual {
			for _, point := range batch.Points() {
				if _, ok := visited[point.String()]; !ok {
					t.Error(fmt.Sprintf("%s: assert_points_sent_to_influx did not send expected point \n expected: %s\n", test.label, point.String()))
				}
			}
		}
	}
}

func assert_error(actual error, test KandiTests, t *testing.T) {
	if actual.Error() != test.expectedError.Error() {
		t.Fatal(fmt.Sprintf("%s: assert_response_error: actual: %s, expected: %s", test.label, actual.Error(), test.expectedError.Error()))
	}
}

func assert_batch(sut *KandiApp, test KandiTests, t *testing.T) {
	acutal := sut.Influx.Client.(*MockClient).sent
	pointsRemaining := test.batchSize
	for _, batch := range acutal {
		if batch.Database() != "mydb" {
			t.Error(fmt.Sprintf("%s: did not set database for batch points. exected: %s, actual: %s", test.label, "mydb", batch.Database()))
		}
		if batch.Precision() != "s" {
			t.Error(fmt.Sprintf("%s: did not set precision for batch points. exected: %s, actual: %s", test.label, "s", batch.Precision()))
		}
		if batch.WriteConsistency() != "any" {
			t.Error("%s: did not set write consistency for batch points. exected: %s, actual: %s", test.label, "any", batch.WriteConsistency())
		}
		if batch.RetentionPolicy() != "autogen" {
			t.Error(fmt.Sprintf("%s: did not set retention policy for batch points. exected: %s, actual: %s", test.label, "autogen", batch.RetentionPolicy()))
		}
		if len(batch.Points()) > test.batchSize {
			t.Error(fmt.Sprintf("%s: had bach size greater than expected. exected: %d, actual: %d", test.label, test.batchSize, len(batch.Points())))
		}
		if len(batch.Points()) < test.batchSize && pointsRemaining >= test.batchSize {
			t.Error(fmt.Sprintf("%s: batched less than expected size. exected: %d, actual: %d", test.label, test.batchSize, len(batch.Points())))
		}
		pointsRemaining = pointsRemaining - len(batch.Points())
	}
}

func Test_Should_Consume_Messages_And_Write_Points_To_Influx(t *testing.T) {
	for _, test := range Kandi_Consume_Tests {
		t.Run(test.label, func(t *testing.T){
			consumer := &MockConsumer{}
			client := &MockClient{nil, nil}

			conf := &Config{}
			conf.Influx = &InfluxConfig{Database: "mydb", Precision:"s", WriteConsistency:"any", RetentionPolicy:"autogen", BatchSize:test.batchSize}
			influx := &Influx{influxConfig: conf.Influx, Client: client}

			sut := &KandiApp{conf, consumer, influx, NewParser()}

			consumer.pointsToReturn = test.input
			consumer.errorToReturn = test.expectedError

			actual := sut.Start(conf)

			assert_error(actual, test, t)
			assert_written_to_influx(sut, test, t)
			assert_committed_offsets(sut, test, t)
			assert_consumer_closed(sut, test, t)
			assert_batch(sut, test, t)
		})
	}
}