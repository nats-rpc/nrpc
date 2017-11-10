package main

import (
	"fmt"
	"log"
	"os"
	"time"

	nats "github.com/nats-io/go-nats"

	// This is the package containing the generated *.pb.go and *.nrpc.go
	// files.
	"github.com/rapidloop/nrpc/examples/helloworld/helloworld"
)

func main() {
	var natsURL = nats.DefaultURL
	if len(os.Args) == 2 {
		natsURL = os.Args[1]
	}
	// Connect to the NATS server.
	nc, err := nats.Connect(natsURL, nats.Timeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// This is our generated client.
	cli := helloworld.NewGreeterClient(nc)

	// Contact the server and print out its response.
	resp, err := cli.SayHello(helloworld.HelloRequest{Name: "world"})
	if err != nil {
		log.Fatal(err)
	}

	// print
	fmt.Printf("Greeting: %s\n", resp.GetMessage())
}
