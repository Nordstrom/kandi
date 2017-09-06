package main

import (
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"github.com/Shopify/sarama"
	"time"
)

func main() {
	go http.ListenAndServe(":8080", nil)
	args := os.Args
	kandi := NewKandi(NewConfig())

	if len(args) > 1 {
		switch args[1] {
			case "backfill": backfill(kandi); break
			default: start(kandi)
		}
	} else {
		start(kandi)
	}
}

func backfill(kandi *Kandi) {
	currentOffsets := GetCurrentoffset(kandi.conf.Kafka)
	postProcessor := func(processedMessages []*sarama.ConsumerMessage){
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
						log.Debug("Kandi has caught up and will now exit successfully")
						os.Exit(0)
					}
				}
			}
		}
	}
	kandi.conf.Kafka.Cluster.Consumer.Offsets.Initial = sarama.OffsetOldest
	kandi.conf.Kafka.Cluster.Consumer.Offsets.Retention = 1 * time.Millisecond
	kandi.conf.Kafka.ConsumerGroup = kandi.conf.Kafka.ConsumerGroup + "-backfill"
	kandi.PostProcessors = []func(processedMessages []*sarama.ConsumerMessage){postProcessor}

	log.Debug("Starting Kandi Backfill")
	kandi.Start()
	log.Info("Stopping Kandi Backfill")
}

func start(kandi *Kandi) {
	log.Debug("Starting Kandi")
	kandi.Start()
	log.Info("Stopping Kandi")
}
