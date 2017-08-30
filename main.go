package main

import (
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"fmt"
)

func main() {
	port := os.Getenv("KANDI_PORT")
	if port == "" {
		port = "8080"
	}
	go http.ListenAndServe(fmt.Sprintf(":%s", port), nil)
	start(NewKandi(NewConfig()))
}

func start(kandi *Kandi) {
	log.Debug("Starting Kandi")
	kandi.Start()
	log.Info("Stopping Kandi")
}
