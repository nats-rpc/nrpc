package main

import (
	"fmt"
	"log"
	"time"

	nats "github.com/nats-io/go-nats"

	// This is the package containing the generated *.pb.go and *.nrpc.go
	// files.
	"github.com/rapidloop/nrpc/examples/helloworld/helloworld"
)

func main() {
	// Connect to the NATS server.
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// This is our generated client.
	cli := helloworld.NewGreeterClient(nc)

	// Contact the server and print out its response.
	resp, err := cli.SayHello(helloworld.HelloRequest{"world"})
	if err != nil {
		log.Fatal(err)
	}

	// print
	fmt.Printf("Greeting: %s\n", resp.Message)

	// Contact the server and print out its response.
	resp2, err := cli.SayHello2(helloworld.HelloRequest{"world"})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Greeting: %s\n", resp2.GetMessage())

	// Contact the server and print out its response.
	resp3, err := cli.SayHello3(helloworld.HelloRequest{"world"})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Greeting: %s\n", resp3)
}
