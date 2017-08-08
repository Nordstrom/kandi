package main

import (
	log "github.com/sirupsen/logrus"
	"net/http"
)

func main() {
	go http.ListenAndServe(":8080", nil)
	start(NewKandi(NewConfig()))
}

func start(kandi *Kandi) {
	log.Debug("Starting Kandi")
	kandi.Start()
	log.Info("Stopping Kandi")
}