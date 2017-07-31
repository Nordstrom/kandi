package main

import (
	"github.com/spf13/viper"
	"time"
	"github.com/bsm/sarama-cluster"
	"bytes"
	"github.com/Shopify/sarama"
	"os"
	saramaLogging "log"
	"io/ioutil"
	"github.com/prometheus/common/log"
)

type Backoff struct {
	Max 		int
	Time	 	int
	Reset 		int
}

type KandiConfig struct {
	Backoff 	*Backoff
}

type Config struct {
	Kandi		*KandiConfig
	Kafka		*KafkaConfig
	Influx		*InfluxConfig
}

func load(input []byte) (*Config) {
	viper.SetEnvPrefix("KANDI")
	viper.AutomaticEnv()
	viper.SetConfigType("yaml")
	err := viper.ReadConfig(bytes.NewBuffer(input))
	if err != nil {
		log.With("Error", err).Error("Unable to read configuration at provided path.")
	}
	return &Config{NewKandiConfig(), NewKafkaConfig(), NewInfluxConfig()}
}

func NewConfig() (*Config) {
	path := os.Getenv("K2I_CONFIG.PATH")
	if path == "" {
		path = "./etx/configuration_example.yaml"
	}
	file, err := ioutil.ReadFile(path)
	if err != nil {
		log.With("Error", err).With("Path", path).Error("Unable to read configuration at provided path.")
	} else {
		log.With("Path", path).Debug("Using initializing with provided configuration.")
	}
	return load(file)
}

func NewKandiConfig() (*KandiConfig) {
	conf := &KandiConfig{&Backoff{}}
	if value, ok := viper.Get("kandi.backoff.max").(int); ok {
		conf.Backoff.Max = value
	}
	if value, ok := viper.Get("kandi.backoff.time").(int); ok {
		conf.Backoff.Time = value
	}
	if value, ok := viper.Get("kandi.backoff.reset").(int); ok {
		conf.Backoff.Reset = value
	}
	return conf
}

func NewInfluxConfig() (*InfluxConfig) {
	conf := &InfluxConfig{}
	if value, ok := viper.Get("influx.url").(string); ok {
		conf.Url = value
	}
	if value, ok := viper.Get("influx.user").(string); ok {
		conf.User = value
	}
	if value, ok := viper.Get("influx.password").(string); ok {
		conf.Password = value
	}
	if value, ok := viper.Get("influx.timeout").(int); ok {
		conf.Timeout = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("influx.userAgent").(string); ok {
		conf.UserAgent = value
	}
	if value, ok := viper.Get("influx.database").(string); ok {
		conf.Database = value
	}
	if value, ok := viper.Get("influx.precision").(string); ok {
		conf.Precision = value
	}
	if value, ok := viper.Get("influx.batchSize").(int); ok {
		conf.BatchSize = value
	}
	if value, ok := viper.Get("influx.writeConsistency").(string); ok {
		conf.WriteConsistency = value
	}
	if value, ok := viper.Get("influx.retentionPolicy").(string); ok {
		conf.RetentionPolicy = value
	}
	return conf
}

func NewKafkaConfig() (*KafkaConfig) {
	conf := KafkaConfig{Cluster: cluster.NewConfig()}
	
	if value, ok := viper.Get("kafka.brokers").(string); ok {
		conf.Brokers = value
	}
	if value, ok := viper.Get("kafka.loggingEnabled").(bool); ok {
		conf.LoggingEnabled = value
		if conf.LoggingEnabled {
			sarama.Logger = saramaLogging.New(os.Stdout, "[Sarama] ", saramaLogging.LstdFlags)
		}
	}
	if value, ok := viper.Get("kafka.topics").(string); ok {
		conf.Topics = value
	}
	if value, ok := viper.Get("kafka.consumerGroup").(string); ok {
		conf.ConsumerGroup = value
	}
	//conf.Cluster.Version = sarama.KafkaVersion{viper.Get("kafka.Version").(int64)}
	if value, ok := viper.Get("kafka.consumer.offsets.initial").(string); ok {
		if value == "newest" {
			conf.Cluster.Consumer.Offsets.Initial = sarama.OffsetNewest
		} else {
			conf.Cluster.Consumer.Offsets.Initial = sarama.OffsetOldest
		}
	}
	if value, ok := viper.Get("kafka.consumer.offsets.retention").(int); ok {
		conf.Cluster.Consumer.Offsets.Retention = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.consumer.offsets.commitInterval").(int); ok {
		conf.Cluster.Consumer.Offsets.CommitInterval = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.consumer.return.errors").(bool); ok {
		conf.Cluster.Consumer.Return.Errors = value
	}
	if value, ok := viper.Get("kafka.consumer.retry.backoff").(int); ok {
		conf.Cluster.Consumer.Retry.Backoff = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.consumer.maxWaitTime").(int); ok {
		conf.Cluster.Consumer.MaxWaitTime = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.consumer.maxProcessingTime").(int); ok {
		conf.Cluster.Consumer.MaxProcessingTime = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.consumer.fetch.max").(int); ok {
		conf.Cluster.Consumer.Fetch.Max = int32(value)
	}
	if value, ok := viper.Get("kafka.consumer.fetch.min").(int); ok {
		conf.Cluster.Consumer.Fetch.Min = int32(value)
	}
	if value, ok := viper.Get("kafka.consumer.fetch.default").(int); ok {
		conf.Cluster.Consumer.Fetch.Default = int32(value)
	}
	if value, ok := viper.Get("kafka.metadata.retry.max").(int); ok {
		conf.Cluster.Metadata.Retry.Max = value
	}
	if value, ok := viper.Get("kafka.metadata.retry.backoff").(int); ok {
		conf.Cluster.Metadata.Retry.Backoff = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.metadata.refreshFrequency").(int); ok {
		conf.Cluster.Metadata.RefreshFrequency = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.net.writeTimeout").(int); ok {
		conf.Cluster.Net.WriteTimeout = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.net.keepAlive").(int); ok {
		conf.Cluster.Net.KeepAlive = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.net.maxOpenRequests").(int); ok {
		conf.Cluster.Net.MaxOpenRequests = value
	}
	if value, ok := viper.Get("kafka.net.readTimeout").(int); ok {
		conf.Cluster.Net.ReadTimeout = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.net.dialTimeout").(int); ok {
		conf.Cluster.Net.DialTimeout = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.net.dialTimeout").(int); ok {
		conf.Cluster.Net.DialTimeout = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.clientID").(string); ok {
		conf.Cluster.ClientID = value
	}
	if value, ok := viper.Get("kafka.channelBufferSize").(int); ok {
		conf.Cluster.ChannelBufferSize = value
	}
	if value, ok := viper.Get("kafka.group.return.notifications").(bool); ok {
		conf.Cluster.Group.Return.Notifications = value
	}
	if value, ok := viper.Get("kafka.group.offsets.retry.max").(int); ok {
		conf.Cluster.Group.Offsets.Retry.Max = value
	}
	if value, ok := viper.Get("kafka.group.partitionStrategy").(string); ok {
		if value == "range" {
			conf.Cluster.Group.PartitionStrategy = cluster.StrategyRange
		} else {
			conf.Cluster.Group.PartitionStrategy = cluster.StrategyRoundRobin
		}
	}
	if value, ok := viper.Get("kafka.group.heartbeat.interval").(int); ok {
		conf.Cluster.Group.Heartbeat.Interval = time.Duration(value) * time.Second
	}
	if value, ok := viper.Get("kafka.group.session.timeout").(int); ok {
		conf.Cluster.Group.Session.Timeout = time.Duration(value) * time.Second
	}
	return &conf
}