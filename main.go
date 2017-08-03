package main

import (
	log "github.com/sirupsen/logrus"
	"net/http"
)

func main() {
	go http.ListenAndServe(":8080", nil)
	start(&Kandi{conf: NewConfig()})
}

func start(kandi *Kandi) {
	log.Debug("Starting Kandi")
	backoff := NewBackoffHandler(kandi.conf)

	for {
		err := kandi.Start()
		if err.Error() == "EndOfTest" {
			log.Info("Stopping Kandi")
			break
		} else {
			log.WithError(err).Error("Kandi exited with error")
			backoff.Handle()
		}
	}
}