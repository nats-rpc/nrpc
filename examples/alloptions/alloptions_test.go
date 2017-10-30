package main

import (
	"context"
	"testing"

	"github.com/nats-io/go-nats"
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

func (BasicServerImpl) MtFullReplyString(
	ctx context.Context, args StringArg,
) (resp string, err error) {
	return args.Arg1, nil
}

func (s BasicServerImpl) MtFullReplyMessage(
	ctx context.Context, args StringArg,
) (resp SimpleStringReply, err error) {
	if ctx.Value("nrpc-svc-clientid").(string) != "me" {
		s.t.Error("Got an invalid nrpc-svc-clientid:", ctx.Value("nrpc-svc-clientid"))
	}
	resp.Reply = args.Arg1
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

	sreply, err := c1.MtFullReplyString(StringArg{"hi"})
	if err != nil {
		t.Fatal(err)
	}
	if sreply != "hi" {
		t.Error("Invalid reply:", sreply)
	}

	r, err = c2.MtFullReplyMessage(StringArg{"hi"})
	if err != nil {
		t.Fatal(err)
	}
	if r.GetReply() != "hi" {
		t.Error("Invalid reply:", r.GetReply())
	}
}
