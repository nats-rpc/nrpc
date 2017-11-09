package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/go-nats"
	"github.com/rapidloop/nrpc"
)

type BasicServerImpl struct {
	t *testing.T
}

func (s BasicServerImpl) MtSimpleReply(
	ctx context.Context, args StringArg,
) (resp SimpleStringReply, err error) {
	if ctx.Value("nrpc-pkg-instance").(string) != "default" {
		s.t.Error("Got an invalid nrpc-pkg-instance:", ctx.Value("nrpc-pkg-instance"))
	}
	resp.Reply = args.Arg1
	return
}

func (s BasicServerImpl) MtWithSubjectParams(
	ctx context.Context, mp1 string, mp2 string,
) (
	resp SimpleStringReply, err error,
) {
	if mp1 != "p1" {
		err = fmt.Errorf("Expects method param mp1 = 'p1', got '%s'", mp1)
	}
	if mp2 != "p2" {
		err = fmt.Errorf("Expects method param mp2 = 'p2', got '%s'", mp2)
	}
	resp.Reply = "Hi"
	return
}

func TestBasicCalls(t *testing.T) {
	c, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	handler1 := NewSvcCustomSubjectHandler(context.Background(), c, BasicServerImpl{t})
	handler2 := NewSvcSubjectParamsHandler(context.Background(), c, BasicServerImpl{t})

	if handler1.Subject() != "root.*.custom_subject.>" {
		t.Fatal("Invalid subject", handler1.Subject())
	}
	if handler2.Subject() != "root.*.svcsubjectparams.*.>" {
		t.Fatal("Invalid subject", handler2.Subject())
	}

	s, err := c.Subscribe(handler1.Subject(), handler1.Handler)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Unsubscribe()
	s, err = c.Subscribe(handler2.Subject(), handler2.Handler)
	if err != nil {
		t.Fatal(err)
	}

	c1 := NewSvcCustomSubjectClient(c, "default")
	c2 := NewSvcSubjectParamsClient(c, "default", "me")

	r, err := c1.MtSimpleReply(StringArg{"hi"})
	if err != nil {
		t.Fatal(err)
	}
	if r.GetReply() != "hi" {
		t.Error("Invalid reply:", r.GetReply())
	}

	r, err = c2.MtWithSubjectParams("p1", "p2")
	if err != nil {
		t.Fatal(err)
	}
	if r.GetReply() != "Hi" {
		t.Error("Invalid reply:", r.GetReply())
	}

	r, err = c2.MtWithSubjectParams("invalid", "p2")
	if err == nil {
		t.Error("Expected an error")
	}
	if nErr, ok := err.(*nrpc.Error); ok {
		if nErr.Type != nrpc.Error_CLIENT || nErr.Message != "Expects method param mp1 = 'p1', got 'invalid'" {
			t.Errorf("Unexpected error %#v", *nErr)
		}
	} else {
		t.Errorf("Expected a nrpc.Error, got %#v", err)
	}
}
