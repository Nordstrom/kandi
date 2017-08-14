package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
	"os"
	"testing"
	"time"
)

var TestEnvInfluxConfig = []byte(`
influx:
  User: george
`)

var TestConfig = []byte(`
influx:
  Url: my-test-url:8082
  User: fred
  Password: fredspassword
  Timeout: 1
  UserAgent: meh
  Database: test-db
  Precision: test-precision
  RetentionPolicy: mypolicy
  WriteConsistency: anywrite

kandi:
  backoff:
    max: 1
    interval: 2
    reset: 3
  batch:
    size: 4
    duration: 5
  loglevel: debug

kafka:
  brokers: test-url:9092
  topics: test-topics
  consumerGroup: test-consumer-group
  loggingEnabled: true
  consumer:
    offsets:
      initial: oldest
      retention: 1
      commitInterval: 2
    return:
      errors: true
    retry:
      backoff: 3
    maxWaitTime: 4
    maxProcessingTime: 5
    fetch:
      max: 6
      min: 7
      default: 8
  metadata:
    retry:
      max: 9
      backoff: 10
    refreshFrequency: 11
  version: 4-4-4-4
  net:
    writeTimeout: 12
    keepAlive: 13
    maxOpenRequests: 14
    readTimeout: 15
    dialTimeout: 16
  clientID: test-client-id
  group:
    return:
      notifications: true
    offsets:
      retry:
        max: 18
    partitionStrategy: range
    heartbeat:
      interval: 19
    session:
      timeout: 20
`)

var EnvironmentConfigTests = []struct {
	label     string
	loadEnv   func()
	removeEnv func()
	test      func(toTest *InfluxConfig, label string, t *testing.T)
}{
	{
		"Insert Influx Url Through Environment Variable Using KANDI_INFLUX.URL",
		func() {
			os.Setenv("KANDI_INFLUX.URL", "my-env-url:8082")
		},
		func() {
			os.Remove("KANDI_INFLUX.URL")
		},
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.Url
			if actual != "my-env-url:8082" {
				t.Error(fmt.Sprintf("%s expected to be my-env-url:8082 but found %s", label, actual))
			}
		},
	},
	{
		"Environment Variable Takes Precidence Over Yaml Using KANDI_INFLUX.USER",
		func() {},
		func() {},
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.User
			if actual != "george" {
				t.Error(fmt.Sprintf("%s expected to be george but found %s", label, actual))
			}
		},
	},
	{
		"Environment Variable Takes Precidence Over Yaml Using KANDI_INFLUX.USER",
		func() {
			os.Setenv("KANDI_INFLUX.USER", "bob")
		},
		func() {
			os.Remove("KANDI_INFLUX.USER")
		},
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.User
			if actual != "bob" {
				t.Error(fmt.Sprintf("%s expected to be bob but found %s", label, actual))
			}
		},
	},
}

var KandiConfigConfigTests = []struct {
	label string
	test  func(toTest *KandiConfig, label string, t *testing.T)
}{
	{
		"kandi.Backoff.Max",
		func(toTest *KandiConfig, label string, t *testing.T) {
			actual := toTest.Backoff.Max
			if actual != 1*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 1 millisecond but found %s", label, actual))
			}
		},
	},
	{
		"kandi.Backoff.Interval",
		func(toTest *KandiConfig, label string, t *testing.T) {
			actual := toTest.Backoff.Interval
			if actual != 2*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 2 milliseconds but found %s", label, actual))
			}
		},
	},
	{
		"kandi.Backoff.Reset",
		func(toTest *KandiConfig, label string, t *testing.T) {
			actual := toTest.Backoff.Reset
			if actual != 3*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 3 milliseconds but found %s", label, actual))
			}
		},
	},
	{
		"kandi.Batch.Size",
		func(toTest *KandiConfig, label string, t *testing.T) {
			actual := toTest.Batch.Size
			if actual != 4 {
				t.Error(fmt.Sprintf("%s expected to be 4 but found %s", label, actual))
			}
		},
	},
	{
		"kandi.Batch.Duration",
		func(toTest *KandiConfig, label string, t *testing.T) {
			actual := toTest.Batch.Duration
			if actual != time.Duration(5)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be %s but found %s", label, time.Duration(5)*time.Millisecond, actual))
			}
		},
	},
	{
		"kandi.loglevel",
		func(toTest *KandiConfig, label string, t *testing.T) {
			actual := log.GetLevel()
			if actual != log.DebugLevel {
				t.Error(fmt.Sprintf("%s expected to be %s but found %s", label, log.DebugLevel, actual))
			}
		},
	},
}

var InfluxConfigTests = []struct {
	label string
	test  func(toTest *InfluxConfig, label string, t *testing.T)
}{
	{
		"influx.Url",
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.Url
			if actual != "my-test-url:8082" {
				t.Error(fmt.Sprintf("%s expected to be my-test-url:8082 but found %s", label, actual))
			}
		},
	},
	{
		"influx.User",
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.User
			if actual != "fred" {
				t.Error(fmt.Sprintf("%s expected to be fred but found %s", label, actual))
			}
		},
	},
	{
		"influx.Password",
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.Password
			if actual != "fredspassword" {
				t.Error(fmt.Sprintf("%s expected to be fredspassword but found %s", label, actual))
			}
		},
	},
	{
		"influx.Timeout",
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.Timeout
			if actual != time.Duration(1)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 1s but found %s", label, actual))
			}
		},
	},
	{
		"influx.UserAgent",
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.UserAgent
			if actual != "meh" {
				t.Error(fmt.Sprintf("%s expected to be meh but found %s", label, actual))
			}
		},
	},
	{
		"influx.Database",
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.Database
			if actual != "test-db" {
				t.Error(fmt.Sprintf("%s expected to be test-db but found %s", label, actual))
			}
		},
	},
	{
		"influx.Precision",
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.Precision
			if actual != "test-precision" {
				t.Error(fmt.Sprintf("%s expected to be test-precision but found %s", label, actual))
			}
		},
	},
	{
		"influx.WriteConsistency",
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.WriteConsistency
			if actual != "anywrite" {
				t.Error(fmt.Sprintf("%s expected to be anywrite but found %s", label, actual))
			}
		},
	},
	{
		"influx.RetentionPolicy",
		func(toTest *InfluxConfig, label string, t *testing.T) {
			actual := toTest.RetentionPolicy
			if actual != "mypolicy" {
				t.Error(fmt.Sprintf("%s expected to be mypolicy but found %s", label, actual))
			}
		},
	},
}

var KafkaConfigTests = []struct {
	label string
	test  func(toTest *KafkaConfig, label string, t *testing.T)
}{
	{
		"kafka.Cluster.Brokers",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Brokers
			if actual != "test-url:9092" {
				t.Error(fmt.Sprintf("%s expected to be test-url:9092 but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Topics",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Topics
			if actual != "test-topics" {
				t.Error(fmt.Sprintf("%s expected to be test-topics but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.ConsumerGroup",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.ConsumerGroup
			if actual != "test-consumer-group" {
				t.Error(fmt.Sprintf("%s expected to be test-consumer-group but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.LoggingEnabled",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.LoggingEnabled
			if actual != true {
				t.Error(fmt.Sprintf("%s expected to be true but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Consumer.Offsets.Initial",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Consumer.Offsets.Initial
			if actual != sarama.OffsetOldest {
				t.Error(fmt.Sprintf("%s expected to be oldest but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Consumer.Offsets.Retention",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Consumer.Offsets.Retention
			if actual != time.Duration(1)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 1 second but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Consumer.Offsets.CommitInterval",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Consumer.Offsets.CommitInterval
			if actual != time.Duration(2)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 2 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Consumer.Return.Errors",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Consumer.Return.Errors
			if actual != true {
				t.Error(fmt.Sprintf("%s expected to be true but found false", label))
			}
		},
	},
	{
		"kafka.Cluster.Consumer.Retry.Backoff",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Consumer.Retry.Backoff
			if actual != time.Duration(3)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 3 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Consumer.MaxWaitTime",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Consumer.MaxWaitTime
			if actual != time.Duration(4)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 4 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Consumer.MaxProcessingTime",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Consumer.MaxProcessingTime
			if actual != time.Duration(5)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 5 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Consumer.Fetch.Max",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Consumer.Fetch.Max
			if actual != 6 {
				t.Error(fmt.Sprintf("%s expected to be 6 but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Consumer.Fetch.Min",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Consumer.Fetch.Min
			if actual != 7 {
				t.Error(fmt.Sprintf("%s expected to be 7 but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Consumer.Fetch.Default",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Consumer.Fetch.Default
			if actual != 8 {
				t.Error(fmt.Sprintf("%s expected to be 8 but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Metadata.Retry.Max",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Metadata.Retry.Max
			if actual != 9 {
				t.Error(fmt.Sprintf("%s expected to be 10 but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Metadata.RefreshFrequency",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Metadata.RefreshFrequency
			if actual != time.Duration(11)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 11 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Net.WriteTimeout",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Net.WriteTimeout
			if actual != 12*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 12 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Net.KeepAlive",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Net.KeepAlive
			if actual != time.Duration(13)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 13 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Net.MaxOpenRequests",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Net.MaxOpenRequests
			if actual != 14 {
				t.Error(fmt.Sprintf("%s expected to be 14 but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Net.ReadTimeout",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Net.ReadTimeout
			if actual != time.Duration(15)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 15 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Net.DialTimeout",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Net.DialTimeout
			if actual != time.Duration(16)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 16 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Net.DialTimeout",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Net.DialTimeout
			if actual != time.Duration(16)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 16 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.ClientID",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.ClientID
			if actual != "test-client-id" {
				t.Error(fmt.Sprintf("%s expected to be test-client-id but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Group.Return.Notifications",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Group.Return.Notifications
			if actual != true {
				t.Error(fmt.Sprintf("%s expected to be true but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Group.Offsets.Retry.Max",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Group.Offsets.Retry.Max
			if actual != 18 {
				t.Error(fmt.Sprintf("%s expected to be 18 but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Group.PartitionStrategy",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Group.PartitionStrategy
			if actual != cluster.StrategyRange {
				t.Error(fmt.Sprintf("%s expected to be cluster.StrategyRange but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Group.Heartbeat.Interval",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Group.Heartbeat.Interval
			if actual != time.Duration(19)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 19 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.Group.Session.Timeout",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.Group.Session.Timeout
			if actual != time.Duration(20)*time.Millisecond {
				t.Error(fmt.Sprintf("%s expected to be 20 seconds but found %d", label, actual))
			}
		},
	},
	{
		"kafka.Cluster.ChannelBufferSize",
		func(toTest *KafkaConfig, label string, t *testing.T) {
			actual := toTest.Cluster.ChannelBufferSize
			if actual != 8 {
				t.Error(fmt.Sprintf("%s expected to be 8 seconds but found %d", label, actual))
			}
		},
	},
}

func Test_InfluxConfig_Configuration_Yaml_Is_Properly_Loaded(t *testing.T) {
	sut := load(TestConfig)
	for _, test := range InfluxConfigTests {
		t.Run(test.label, func(t *testing.T) {
			log.SetLevel(log.PanicLevel)
			test.test(sut.Influx, test.label, t)
		})
	}
}

func Test_KafkaConfig_Configuration_Yaml_Is_Properly_Loaded(t *testing.T) {
	sut := load(TestConfig)
	for _, test := range KafkaConfigTests {
		t.Run(test.label, func(t *testing.T) {
			log.SetLevel(log.PanicLevel)
			test.test(sut.Kafka, test.label, t)
		})
	}
}

func Test_KandiConfig_Configuration_Yaml_Is_Properly_Loaded(t *testing.T) {
	sut := load(TestConfig)
	for _, test := range KandiConfigConfigTests {
		t.Run(test.label, func(t *testing.T) {
			test.test(sut.Kandi, test.label, t)
		})
	}
}

func Test_environment_override(t *testing.T) {
	for _, test := range EnvironmentConfigTests {
		test.loadEnv()
		log.SetLevel(log.PanicLevel)
		sut := load(TestEnvInfluxConfig)
		test.test(sut.Influx, test.label, t)
		test.removeEnv()
	}
}
