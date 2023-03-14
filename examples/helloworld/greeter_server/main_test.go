package main

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	// This is the package containing the generated *.pb.go and *.nrpc.go
	// files.
	"github.com/nats-rpc/nrpc/examples/helloworld/helloworld"
)

func TestBasic(t *testing.T) {
	// Connect to the NATS server.
	nc, err := nats.Connect(natsURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// Our server implementation.
	s := &server{}

	// The NATS handler from the helloworld.nrpc.proto file.
	h := helloworld.NewGreeterHandler(context.TODO(), nc, s)

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
