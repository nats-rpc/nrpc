package nrpc

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/logger"
	"github.com/nats-io/gnatsd/server"
)

var NatsURL string

func TestMain(m *testing.M) {
	gnatsd := server.New(&server.Options{Port: server.RANDOM_PORT})
	gnatsd.SetLogger(
		logger.NewStdLogger(false, false, false, false, false),
		false, false)
	go gnatsd.Start()
	defer gnatsd.Shutdown()

	if !gnatsd.ReadyForConnections(time.Second) {
		log.Fatal("Cannot start the gnatsd server")
	}
	NatsURL = "nats://" + gnatsd.Addr().String()

	os.Exit(m.Run())
}
