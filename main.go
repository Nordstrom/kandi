package main

import (
	"github.com/prometheus/common/log"
	"time"
)

func main() {
	conf := NewConfig()
	kandi := KandiApp{conf:conf}
	start(conf, &kandi)
}

func start(conf *Config, kandi Kandi) {
	log.Debug("Starting Application")
	currentBackoff := conf.Kandi.Backoff.Time
	lastBackoff := time.Now()
	for {
		err := kandi.Start(conf)
		if err ==  nil {
			log.Info("Stopping application")
			break
		} else {
			log.With("error", err.Error()).Error("Application exited with error")
			if conf.Kandi.Backoff.Max >= 0 {
				if conf.Kandi.Backoff.Reset > 0 && time.Since(lastBackoff) >= time.Duration(conf.Kandi.Backoff.Reset) {
					currentBackoff = conf.Kandi.Backoff.Time
				}
				log.With("time", currentBackoff).Info("Backing off")
				time.Sleep(time.Duration(currentBackoff) * time.Second)
				if conf.Kandi.Backoff.Max > 0 && currentBackoff * 2 > conf.Kandi.Backoff.Max {
					currentBackoff = conf.Kandi.Backoff.Max
				} else {
					currentBackoff = currentBackoff * 2
				}
				lastBackoff = time.Now()
				log.Info("Restarting Application")
			}
		}
	}
}