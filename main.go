package main

import (
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"github.com/Shopify/sarama"
	"time"
)

func main() {
	port := os.Getenv("KANDI_PORT")
	if port != "" {
		go http.ListenAndServe(":" + port, nil)
	} else {
		go http.ListenAndServe(":8080", nil)
	}
	args := os.Args


	if len(args) > 1 {
		switch args[1] {

			case "backfill":
				backfill(NewKandi(NewConfig()))
				break
			default:
				kandi := NewKandi(NewConfig())
				start(kandi)
		}
	} else {
		kandi := NewKandi(NewConfig())
		start(kandi)
	}
}

func backfill(kandi *Kandi) {
	currentOffsets := GetCurrentoffset(kandi.conf.Kafka)
	postProcessor := func(processedMessages []*sarama.ConsumerMessage) bool {
		for _, message := range processedMessages {
			if message != nil {
				if value, ok := currentOffsets[message.Partition]; ok && !value.finished && message.Offset > value.mark {
					value.finished = true
					currentOffsets[message.Partition] = value

					finished := true
					for _, partitionOffset := range currentOffsets {
						if !partitionOffset.finished {
							finished = false
							break
						}
					}
					if finished {
						return true
					}
				}
			}
		}
		return false
	}
	kandi.conf.Kafka.Cluster.Consumer.Offsets.Initial = sarama.OffsetOldest
	kandi.conf.Kafka.Cluster.Consumer.Offsets.Retention = 1 * time.Millisecond
	kandi.conf.Kafka.ConsumerGroup = kandi.conf.Kafka.ConsumerGroup + "-backfill"
	kandi.PostProcessors = []func(processedMessages []*sarama.ConsumerMessage) bool {postProcessor}

	log.Debug("Starting Kandi Backfill")
	kandi.Start()
	log.Info("Stopping Kandi Backfill")

	for {
		time.Sleep(time.Duration(24) * time.Hour)
	}
}

func start(kandi *Kandi) {
	log.Debug("Starting Kandi")
	kandi.Start()
	log.Info("Stopping Kandi")
}
