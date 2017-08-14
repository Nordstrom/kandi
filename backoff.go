package main

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type BackoffHandler struct {
	name           string
	conf           *Config
	last           time.Time
	currentBackoff time.Duration
}

func NewBackoffHandler(name string, conf *Config) *BackoffHandler {
	return &BackoffHandler{name, conf, time.Now(), conf.Kandi.Backoff.Interval}
}

func (handler *BackoffHandler) Handle() {

	if handler.conf.Kandi.Backoff.Interval >= 0 {
		handler.do()
		handler.update()
		handler.reset()
	}
}

func (handler *BackoffHandler) reset() {
	if handler.conf.Kandi.Backoff.Reset > 0 && time.Since(handler.last) >= handler.conf.Kandi.Backoff.Reset {
		handler.currentBackoff = handler.conf.Kandi.Backoff.Interval
		handler.last = time.Now()
	}
}

func (handler *BackoffHandler) do() {
	log.WithField("name", handler.name).WithFields(log.Fields{"time": handler.currentBackoff}).Info("Backing off")
	MetricsBackoff(handler.name)
	handler.last = time.Now()
	time.Sleep(handler.currentBackoff)
}

func (handler *BackoffHandler) update() {
	if handler.conf.Kandi.Backoff.Max > 0 && handler.currentBackoff*2 >= handler.conf.Kandi.Backoff.Max {
		handler.currentBackoff = handler.conf.Kandi.Backoff.Max
	} else {
		handler.currentBackoff = handler.currentBackoff * 2
	}
}
