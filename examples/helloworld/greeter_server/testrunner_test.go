package main

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/logger"
	natsServer "github.com/nats-io/gnatsd/server"
)

var natsURL string

func TestMain(m *testing.M) {
	gnatsd := natsServer.New(&natsServer.Options{Port: natsServer.RANDOM_PORT})
	gnatsd.SetLogger(
		logger.NewStdLogger(false, false, false, false, false),
		false, false)
	go gnatsd.Start()
	defer gnatsd.Shutdown()

	if !gnatsd.ReadyForConnections(time.Second) {
		log.Fatal("Cannot start the gnatsd server")
	}
	natsURL = "nats://" + gnatsd.Addr().String()

	os.Exit(m.Run())
}
