package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/rapidloop/nrpc"
)

type BasicServerImpl struct {
	t        *testing.T
	handler  *SvcCustomSubjectHandler
	handler2 *SvcSubjectParamsHandler
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

func (s BasicServerImpl) MtVoidReply(
	ctx context.Context, args StringArg,
) (err error) {
	if args.GetArg1() == "please fail" {
		return errors.New("Error")
	}
	return nil
}

func (s BasicServerImpl) MtStreamedReply(
	ctx context.Context, req StringArg, send func(rep SimpleStringReply),
) error {
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
	time.Sleep(2 * time.Second)
	send(SimpleStringReply{"msg1"})
	time.Sleep(250 * time.Millisecond)
	send(SimpleStringReply{"msg2"})
	time.Sleep(250 * time.Millisecond)
	send(SimpleStringReply{"msg3"})
	time.Sleep(250 * time.Millisecond)
	return nil
}

func (s BasicServerImpl) MtVoidReqStreamedReply(
	ctx context.Context, send func(rep SimpleStringReply),
) error {
	time.Sleep(2 * time.Second)
	send(SimpleStringReply{"hi"})
	return nil
}

func (s BasicServerImpl) MtNoReply(ctx context.Context) {
	s.t.Log("Will publish to MtNoRequest")
	s.handler.MtNoRequestPublish("default", SimpleStringReply{"Hi there"})
	s.handler2.MtNoRequestWParamsPublish("default", "me", "mtvalue", SimpleStringReply{"Hi there"})
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

func TestAll(t *testing.T) {
	c, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	handler1 := NewSvcCustomSubjectHandler(context.Background(), c, BasicServerImpl{t, nil, nil})
	impl := BasicServerImpl{t, handler1, nil}
	handler2 := NewSvcSubjectParamsHandler(context.Background(), c, &impl)
	impl.handler2 = handler2

	if handler1.Subject() != "root.*.custom_subject.>" {
		t.Fatal("Invalid subject", handler1.Subject())
	}
	if handler2.Subject() != "root.*.svcsubjectparams.*.>" {
		t.Fatal("Invalid subject", handler2.Subject())
	}

	for _, encoding := range []string{"protobuf", "json"} {
		t.Run("Encoding_"+encoding, func(t *testing.T) {

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

			c1.Encoding = encoding
			c2.Encoding = encoding

			r, err := c1.MtSimpleReply(StringArg{"hi"})
			if err != nil {
				t.Fatal(err)
			}
			if r.GetReply() != "hi" {
				t.Error("Invalid reply:", r.GetReply())
			}

			if err := c1.MtVoidReply(StringArg{"hi"}); err != nil {
				t.Error("Unexpected error:", err)
			}

			err = c1.MtVoidReply(StringArg{"please fail"})
			if err == nil {
				t.Error("Expected an error")
			}

			t.Run("StreamedReply", func(t *testing.T) {
				t.Run("Simple", func(t *testing.T) {
					var resList []string
					err := c1.MtStreamedReply(
						context.Background(),
						StringArg{"arg"},
						func(ctx context.Context, rep SimpleStringReply) {
							resList = append(resList, rep.GetReply())
						})
					if err != nil {
						t.Fatal(err)
					}
					if resList[0] != "msg1" {
						t.Errorf("Expected 'msg1', got '%s'", resList[0])
					}
					if resList[1] != "msg2" {
						t.Errorf("Expected 'msg2', got '%s'", resList[1])
					}
					if resList[2] != "msg3" {
						t.Errorf("Expected 'msg3', got '%s'", resList[2])
					}
				})

				t.Run("Error", func(t *testing.T) {
					err := c1.MtStreamedReply(context.Background(),
						StringArg{"please fail"},
						func(ctx context.Context, rep SimpleStringReply) {
							t.Fatal("Should not receive anything")
						})
					if err == nil {
						t.Fatal("Expected an error, got nil")
					}
				})

				t.Run("Cancel", func(t *testing.T) {
					ctx, _ := context.WithTimeout(context.Background(), 7*time.Second)
					err := c1.MtStreamedReply(ctx,
						StringArg{"very long call"},
						func(context.Context, SimpleStringReply) {
							t.Fatal("Should not receive anything")
						})
					if err != nrpc.ErrCanceled {
						t.Fatal("Expects a ErrCanceled error, got ", err)
					}
				})

				t.Run("VoidRequest", func(t *testing.T) {
					ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
					err := c1.MtVoidReqStreamedReply(ctx, func(context.Context, SimpleStringReply) {})
					if err != nil {
						fmt.Print(err)
						t.Error(err)
					}
				})
			})

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

			t.Run("NoRequest method with params", func(t *testing.T) {
				sub, err := c2.MtNoRequestWParamsSubscribeSync(
					"mtvalue",
				)
				if err != nil {
					t.Fatal(err)
				}
				defer sub.Unsubscribe()
				c2.MtNoReply()
				reply, err := sub.Next(time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if reply.GetReply() != "Hi there" {
					t.Errorf("Expected 'Hi there', got %s", reply.GetReply())
				}
			})
			t.Run("NoRequest method subscriptions", func(t *testing.T) {
				if encoding != "protobuf" {
					t.Skip()
				}
				fmt.Println("Subscribing")
				sub1, err := c1.MtNoRequestSubscribeSync()
				if err != nil {
					t.Fatal(err)
				}
				defer sub1.Unsubscribe()
				repChan := make(chan string, 2)
				sub2, err := c1.MtNoRequestSubscribe(func(msg SimpleStringReply) {
					repChan <- msg.GetReply()
				})
				if err != nil {
					t.Fatal(err)
				}
				defer sub2.Unsubscribe()
				msgChan, sub3, err := c1.MtNoRequestSubscribeChan()
				if err != nil {
					t.Fatal(err)
				}
				defer sub3.Unsubscribe()
				go func() {
					msg := <-msgChan
					repChan <- msg.GetReply()
				}()

				err = c2.MtNoReply()
				if err != nil {
					t.Fatal(err)
				}
				msg, err := sub1.Next(time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if msg.GetReply() != "Hi there" {
					t.Errorf("Expected 'Hi there', got '%s'", msg.GetReply())
				}
				for _ = range []int{0, 1} {
					select {
					case rep := <-repChan:
						if rep != "Hi there" {
							t.Errorf("Expected 'Hi there', got '%s'", rep)
						}
					case <-time.After(time.Second):
						t.Fatal("timeout")
					}
				}
			})

		})
	}
}
