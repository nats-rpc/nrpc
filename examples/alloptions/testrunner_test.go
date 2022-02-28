package main

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats-server/v2/server"
)

var natsURL string

func TestMain(m *testing.M) {
	gnatsd, err := server.NewServer(&server.Options{Port: server.RANDOM_PORT})
	if err != nil {
		os.Exit(1)
	}
	gnatsd.SetLogger(
		logger.NewStdLogger(false, false, false, false, false),
		false, false)
	go gnatsd.Start()
	defer gnatsd.Shutdown()

	if !gnatsd.ReadyForConnections(time.Second) {
		log.Fatal("Cannot start the nats server")
	}
	natsURL = "nats://" + gnatsd.Addr().String()

	os.Exit(m.Run())
}
