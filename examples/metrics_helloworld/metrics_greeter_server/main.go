package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/nats-io/nats.go"

	// This is the package containing the generated *.pb.go and *.nrpc.go
	// files.
	"github.com/T-J-L/nrpc/examples/metrics_helloworld/helloworld"

	// If you've used the prometheus plugin when generating the code, you
	// can import the HTTP handler of Prometheus to serve up the metrics.
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// server implements the helloworld.GreeterServer interface.
type server struct{}

// SayHello is an implementation of the SayHello method from the definition of
// the Greeter service.
func (s *server) SayHello(ctx context.Context, req *helloworld.HelloRequest) (resp *helloworld.HelloReply, err error) {
	resp.Message = "Hello " + req.Name
	if rand.Intn(10) < 7 { // will fail 70% of the time
		err = errors.New("random failure simulated")
	}
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond) // random delay
	return
}

func main() {
	// Connect to the NATS server.
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Our server implementation.
	s := &server{}
	rand.Seed(time.Now().UnixNano())

	// The NATS handler from the helloworld.nrpc.proto file.
	h := helloworld.NewGreeterHandler(context.TODO(), nc, s)

	// Start a NATS subscription using the handler. You can also use the
	// QueueSubscribe() method for a load-balanced set of servers.
	sub, err := nc.Subscribe(h.Subject(), h.Handler)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	// Do this block only if you generated the code with the prometheus plugin.
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":6060", nil)

	// Keep running until ^C.
	fmt.Println("server is running, ^C quits.")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	close(c)
}
