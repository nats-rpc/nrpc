package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-rpc/nrpc"
)

//go:generate protoc -I. -I ../../.. --go_out . --nrpc_out . alloptions.proto

type ServerImpl struct {
	handler  *SvcCustomSubjectHandler
	handler2 *SvcSubjectParamsHandler
}

func (s ServerImpl) MtSimpleReply(
	ctx context.Context, args *StringArg,
) (*SimpleStringReply, error) {
	fmt.Println("MtSimpleReply: " + args.GetArg1())
	if instance := nrpc.GetRequest(ctx).PackageParam("instance"); instance != "default" {
		return nil, fmt.Errorf("Got an invalid package param instance: '%s'", instance)
	}
	return &SimpleStringReply{Reply: args.Arg1}, nil
}

func (s ServerImpl) MtVoidReply(
	ctx context.Context, args *StringArg,
) error {
	fmt.Println("MtVoidReply: " + args.GetArg1())
	if args.GetArg1() == "please fail" {
		return errors.New("Error")
	}
	return nil
}

func (s ServerImpl) MtStreamedReply(
	ctx context.Context, req *StringArg, send func(rep *SimpleStringReply),
) error {
	fmt.Println("MtStreamedReply: " + req.GetArg1())
	if req.GetArg1() == "please fail" {
		panic("Failing")
	}
	if req.GetArg1() == "very long call" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Minute):
			time.Sleep(time.Minute)
			return nil
		}
	}
	time.Sleep(time.Second)
	send(&SimpleStringReply{Reply: "msg1"})
	time.Sleep(250 * time.Millisecond)
	send(&SimpleStringReply{Reply: "msg2"})
	time.Sleep(250 * time.Millisecond)
	send(&SimpleStringReply{Reply: "msg3"})
	time.Sleep(250 * time.Millisecond)
	return nil
}

func (s ServerImpl) MtVoidReqStreamedReply(
	ctx context.Context, send func(rep *SimpleStringReply),
) error {
	fmt.Println("MtVoidReqStreamedReply")
	time.Sleep(2 * time.Second)
	send(&SimpleStringReply{Reply: "hi"})
	return nil
}

func (s ServerImpl) MtNoReply(ctx context.Context) {
	fmt.Println("MtNoReply")
	if err := s.handler.MtNoRequestPublish("default", &SimpleStringReply{Reply: "Hi there"}); err != nil {
		fmt.Println(err)
	}
	if err := s.handler2.MtNoRequestWParamsPublish("default", "me", "mtvalue", &SimpleStringReply{Reply: "Hi there"}); err != nil {
		fmt.Println(err)
	}
}

func (s ServerImpl) MtWithSubjectParams(
	ctx context.Context, mp1 string, mp2 string,
) (*SimpleStringReply, error) {
	fmt.Println("MtWithSubjectParams: ", mp1, mp2)
	var err error
	if mp1 != "p1" {
		err = fmt.Errorf("Expects method param mp1 = 'p1', got '%s'", mp1)
	}
	if mp2 != "p2" {
		err = fmt.Errorf("Expects method param mp2 = 'p2', got '%s'", mp2)
	}
	return &SimpleStringReply{Reply: "Hi"}, err
}

func (s ServerImpl) MtStreamedReplyWithSubjectParams(
	ctx context.Context, mp1 string, mp2 string, send func(rep *SimpleStringReply),
) error {
	fmt.Println("MtStreamedReplyWithSubjectParams: ", mp1, mp2)
	send(&SimpleStringReply{Reply: mp1})
	send(&SimpleStringReply{Reply: mp2})
	return nil
}

func main() {
	c, err := nats.Connect("localhost:4222")
	if err != nil {
		fmt.Println(err)
		return
	}

	pool := nrpc.NewWorkerPool(context.Background(), 2, 5, 4*time.Second)

	impl := ServerImpl{nil, nil}
	handler1 := NewSvcCustomSubjectConcurrentHandler(pool, c, &impl)
	handler2 := NewSvcSubjectParamsConcurrentHandler(pool, c, &impl)
	impl.handler = handler1
	impl.handler2 = handler2

	s, err := c.QueueSubscribe(handler1.Subject(), "queue", handler1.Handler)
	if err != nil {
		panic(err)
	}
	defer s.Unsubscribe()
	s, err = c.QueueSubscribe(handler2.Subject(), "queue", handler2.Handler)
	if err != nil {
		panic(err)
	}
	defer s.Unsubscribe()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Blocking, press ctrl+c to continue...")
	<-done // Will block here until user hits ctrl+c
}
