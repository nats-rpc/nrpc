package main

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	// This is the package containing the generated *.pb.go and *.nrpc.go
	// files.
	"github.com/T-J-L/nrpc/examples/helloworld/helloworld"
)

func TestBasic(t *testing.T) {
	s := natsserver.RunRandClientPortServer()
	defer s.Shutdown()
	// Connect to the NATS server.
	nc, err := nats.Connect(s.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// Our server implementation.
	srv := &server{}

	// The NATS handler from the helloworld.nrpc.proto file.
	h := helloworld.NewGreeterHandler(context.TODO(), nc, srv)

	// Start a NATS subscription using the handler. You can also use the
	// QueueSubscribe() method for a load-balanced set of servers.
	sub, err := nc.Subscribe(h.Subject(), h.Handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// This is our generated client.
	cli := helloworld.NewGreeterClient(nc)

	// Contact the server and print out its response.
	resp, err := cli.SayHello(&helloworld.HelloRequest{Name: "world"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetMessage() != "Hello world" {
		t.Fatalf("unexpected message: %s", resp.GetMessage())
	}
}
