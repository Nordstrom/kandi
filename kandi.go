package main

import (
	"github.com/prometheus/common/log"
)

type KandiApp struct {
	conf 		*Config
	Consumer	Consumer
	Influx		*Influx
	Parser		Parser
}

func (k *KandiApp) initialize() (error) {
	if k.Influx ==  nil {
		influx, err := NewInflux(k.conf.Influx)
		if err != nil  {
			return err
		}
		influx.Test()
		k.Influx = influx
	}
	if k.Consumer == nil {
		consumer, err := NewKafkaConsumer(k.conf.Kafka)
		if err != nil {
			return err
		}
		k.Consumer = consumer
	}
	if k.Parser == nil {
		k.Parser = NewParser()
	}
	return nil
}

type Kandi interface {
	Start(conf *Config) (error)
}

func (k *KandiApp) Start(conf *Config) (error) {
	k.initialize()
	defer k.Consumer.Close()
	for {
		message, notification, err := k.Consumer.Consume()
		if err != nil {
			return err
		}
		if notification != nil {
			println("Rebalanced")
		}
		if message != nil && len(message.Value) != 0 {

			log.With("database", k.conf.Influx.Database).Info("Creating new batch point")

			parsed := k.Parser.parse(message.Value)
			if parsed.Err == nil {
				log.With("database", k.conf.Influx.Database).With("point", parsed.Point).Info("Adding point to batch points")
				err := k.Influx.Write(parsed.Point)
				if err != nil {
					return err
				}
			}
			k.Consumer.MarkOffset(message)
		}
	}
	return nil
}