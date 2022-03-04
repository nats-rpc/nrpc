package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/T-J-L/nrpc"
)

type TestingLogWriter struct {
	t *testing.T
}

func (w TestingLogWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

type BasicServerImpl struct {
	t        *testing.T
	handler  *SvcCustomSubjectHandler
	handler2 *SvcSubjectParamsHandler
}

func (s *BasicServerImpl) MtSimpleReply(ctx context.Context, args *StringArg) (resp *SimpleStringReply, err error) {
	if instance := nrpc.GetRequest(ctx).PackageParam("instance"); instance != "default" {
		s.t.Errorf("Got an invalid package param instance: '%s'", instance)
	}
	return &SimpleStringReply{Reply: args.Arg1}, nil
}

func (s *BasicServerImpl) MtVoidReply(ctx context.Context, args *StringArg) (err error) {
	if args.GetArg1() == "please fail" {
		return errors.New("Error")
	}
	return nil
}

func (s *BasicServerImpl) MtStreamedReply(ctx context.Context, req *StringArg, send func(rep SimpleStringReply)) error {
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
	send(SimpleStringReply{Reply: "msg1"})
	time.Sleep(250 * time.Millisecond)
	send(SimpleStringReply{Reply: "msg2"})
	time.Sleep(250 * time.Millisecond)
	send(SimpleStringReply{Reply: "msg3"})
	time.Sleep(250 * time.Millisecond)
	return nil
}

func (s BasicServerImpl) MtVoidReqStreamedReply(ctx context.Context, send func(rep SimpleStringReply)) error {
	time.Sleep(2 * time.Second)
	send(SimpleStringReply{Reply: "hi"})
	return nil
}

func (s BasicServerImpl) MtNoReply(ctx context.Context) {
	s.t.Log("Will publish to MtNoRequest")
	s.handler.MtNoRequestPublish("default", SimpleStringReply{Reply: "Hi there"})
	s.handler2.MtNoRequestWParamsPublish("default", "me", "mtvalue", SimpleStringReply{Reply: "Hi there"})
}

func (s BasicServerImpl) MtWithSubjectParams(ctx context.Context, mp1, mp2 string) (resp *SimpleStringReply, err error) {
	if mp1 != "p1" {
		err = fmt.Errorf("Expects method param mp1 = 'p1', got '%s'", mp1)
	}
	if mp2 != "p2" {
		err = fmt.Errorf("Expects method param mp2 = 'p2', got '%s'", mp2)
	}
	return &SimpleStringReply{Reply: "Hi"}, err
}

func (s BasicServerImpl) MtStreamedReplyWithSubjectParams(ctx context.Context, mp1 string, mp2 string, send func(rep SimpleStringReply)) error {
	send(SimpleStringReply{Reply: mp1})
	send(SimpleStringReply{Reply: mp2})
	return nil
}

func TestAll(t *testing.T) {
	s := natsserver.RunRandClientPortServer()
	defer s.Shutdown()

	c, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}

	log.SetOutput(TestingLogWriter{t})

	t.Run("MultiProtocolPublish", func(t *testing.T) {
		log.SetOutput(TestingLogWriter{t})
		handler := NewSvcCustomSubjectHandler(context.Background(), c, &BasicServerImpl{t, nil, nil})
		handler.SetEncodings([]string{"protobuf", "json"})

		c1 := NewSvcCustomSubjectClient(c, "default")

		for _, protocol := range []string{"protobuf", "json"} {
			t.Run(protocol, func(t *testing.T) {
				c1.Encoding = protocol
				if protocol == "protobuf" {
					require.Equal(t, "root.default.custom_subject.mtnorequest", c1.MtNoRequestSubject())
				} else {
					require.Equal(t, "root.default.custom_subject.mtnorequest."+protocol, c1.MtNoRequestSubject())
				}
				sub, err := c1.MtNoRequestSubscribeSync()
				if err != nil {
					t.Fatal(err)
				}
				defer sub.Unsubscribe()

				if err := handler.MtNoRequestPublish(
					"default", SimpleStringReply{Reply: "test"},
				); err != nil {
					t.Fatal(t)
				}

				msg, err := sub.Next(time.Second)
				if err != nil {
					t.Fatal(err)
				}
				require.Equal(t, "test", msg.GetReply())
			})
		}
	})

	t.Run("NoConcurrency", func(t *testing.T) {
		log.SetOutput(TestingLogWriter{t})
		handler1 := NewSvcCustomSubjectHandler(context.Background(), c, &BasicServerImpl{t, nil, nil})
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
			t.Run("Encoding_"+encoding, commonTests(c, handler1, handler2, encoding))
		}
	})

	t.Run("WithConcurrency", func(t *testing.T) {
		log.SetOutput(TestingLogWriter{t})
		pool := nrpc.NewWorkerPool(context.Background(), 2, 5, 4*time.Second)

		handler1 := NewSvcCustomSubjectConcurrentHandler(pool, c, &BasicServerImpl{t, nil, nil})
		impl := BasicServerImpl{t, handler1, nil}
		handler2 := NewSvcSubjectParamsConcurrentHandler(pool, c, &impl)
		impl.handler2 = handler2

		if handler1.Subject() != "root.*.custom_subject.>" {
			t.Fatal("Invalid subject", handler1.Subject())
		}
		if handler2.Subject() != "root.*.svcsubjectparams.*.>" {
			t.Fatal("Invalid subject", handler2.Subject())
		}

		for _, encoding := range []string{"protobuf", "json"} {
			t.Run("Encoding_"+encoding, commonTests(c, handler1, handler2, encoding))
		}

		// Now a few tests very specific to concurrency handling

		s, err := c.QueueSubscribe(handler1.Subject(), "queue", handler1.Handler)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Unsubscribe()
		s, err = c.QueueSubscribe(handler2.Subject(), "queue", handler2.Handler)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Unsubscribe()

		c1 := NewSvcCustomSubjectClient(c, "default")
		//c2 := NewSvcSubjectParamsClient(c, "default", "me")

		t.Run("Concurrent Stream calls", func(t *testing.T) {
			log.SetOutput(TestingLogWriter{t})
			var resList []string
			var wg sync.WaitGroup
			var resChan = make(chan string, 2)
			go func() {
				for r := range resChan {
					resList = append(resList, r)
				}
			}()
			for i := 0; i != 2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := c1.MtStreamedReply(
						context.Background(),
						StringArg{Arg1: "arg"},
						func(ctx context.Context, rep *SimpleStringReply) {
							fmt.Println("received", rep)
							resChan <- rep.GetReply()
						})
					if err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			close(resChan)

			expectsStringSlice(t,
				[]string{"msg1", "msg1", "msg2", "msg2", "msg3", "msg3"},
				resList)
		})

		t.Run("Too many concurrent Stream calls", func(t *testing.T) {
			log.SetOutput(TestingLogWriter{t})
			pool.SetMaxPendingDuration(2 * time.Second)
			var resList []string
			var wg sync.WaitGroup
			var resChan = make(chan string, 2)
			go func() {
				for r := range resChan {
					resList = append(resList, r)
				}
			}()
			for i := 0; i != 7; i++ {
				wg.Add(1)
				time.Sleep(50 * time.Millisecond)
				go func(i int) {
					defer wg.Done()
					err := c1.MtStreamedReply(
						context.Background(),
						StringArg{Arg1: "arg"},
						func(ctx context.Context, rep *SimpleStringReply) {
							fmt.Println("received", rep)
							resChan <- rep.GetReply()
						})
					if i >= 4 {
						if nrpcErr, ok := err.(*nrpc.Error); !ok || nrpcErr.Type != nrpc.Error_SERVERTOOBUSY {
							t.Errorf("Should get a SERVERTOOBUSY error, got %v", err)
						}
					} else if err != nil {
						t.Errorf("Should succeed but got: %s", err)
					}
				}(i)
			}

			// Wait so the 7 calls are already queued
			time.Sleep(200 * time.Millisecond)

			// The 7th call should get a SERVERTOOBUSY error
			err := c1.MtStreamedReply(
				context.Background(),
				StringArg{Arg1: "arg"},
				func(ctx context.Context, rep *SimpleStringReply) {
					fmt.Println("received", rep)
				})
			if err == nil {
				t.Error("Should get an error")
			} else if nrpcErr, ok := err.(*nrpc.Error); ok {
				if nrpcErr.Type != nrpc.Error_SERVERTOOBUSY {
					t.Errorf("Should get a SERVERTOOBUSY, got %v", nrpcErr.Type)
				}
			} else {
				t.Errorf("Should get a nrpcError, got %v", err)
			}

			wg.Wait()
			close(resChan)
		})

		pool.Close(time.Second)
	})
}

func commonTests(
	conn *nats.Conn,
	handler1 *SvcCustomSubjectHandler,
	handler2 *SvcSubjectParamsHandler,
	encoding string,
) func(t *testing.T) {
	return func(t *testing.T) {
		handler1.SetEncodings([]string{encoding})
		handler2.SetEncodings([]string{encoding})
		s, err := conn.QueueSubscribe(handler1.Subject(), "queue", handler1.Handler)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Unsubscribe()
		s, err = conn.QueueSubscribe(handler2.Subject(), "queue", handler2.Handler)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Unsubscribe()

		c1 := NewSvcCustomSubjectClient(conn, "default")
		c2 := NewSvcSubjectParamsClient(conn, "default", "me")

		c1.Encoding = encoding
		c2.Encoding = encoding

		r, err := c1.MtSimpleReply(&StringArg{Arg1: "hi"})
		if err != nil {
			t.Fatal(err)
		}
		if r.GetReply() != "hi" {
			t.Error("Invalid reply:", r.GetReply())
		}

		if err := c1.MtVoidReply(&StringArg{Arg1: "hi"}); err != nil {
			t.Error("Unexpected error:", err)
		}

		err = c1.MtVoidReply(&StringArg{Arg1: "please fail"})
		if err == nil {
			t.Error("Expected an error")
		}

		t.Run("StreamedReply", func(t *testing.T) {
			log.SetOutput(TestingLogWriter{t})
			t.Run("Simple", func(t *testing.T) {
				log.SetOutput(TestingLogWriter{t})
				var resList []string
				err := c1.MtStreamedReply(
					context.Background(),
					StringArg{Arg1: "arg"},
					func(ctx context.Context, rep *SimpleStringReply) {
						fmt.Println("received", rep)
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
				log.SetOutput(TestingLogWriter{t})
				err := c1.MtStreamedReply(context.Background(),
					StringArg{Arg1: "please fail"},
					func(ctx context.Context, rep *SimpleStringReply) {
						t.Fatal("Should not receive anything")
					})
				if err == nil {
					t.Fatal("Expected an error, got nil")
				}
			})

			t.Run("Cancel", func(t *testing.T) {
				log.SetOutput(TestingLogWriter{t})
				ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
				defer cancel()
				err := c1.MtStreamedReply(ctx,
					StringArg{Arg1: "very long call"},
					func(context.Context, *SimpleStringReply) {
						t.Fatal("Should not receive anything")
					})
				if err != nrpc.ErrCanceled {
					t.Fatal("Expects a ErrCanceled error, got ", err)
				}
			})

			t.Run("VoidRequest", func(t *testing.T) {
				log.SetOutput(TestingLogWriter{t})
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err := c1.MtVoidReqStreamedReply(ctx, func(context.Context, *SimpleStringReply) {})
				if err != nil {
					fmt.Print(err)
					t.Error(err)
				}
			})
		})

		t.Run("SubjectParams", func(t *testing.T) {
			log.SetOutput(TestingLogWriter{t})
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
		})

		t.Run("StreamedReply with SubjectParams", func(t *testing.T) {
			log.SetOutput(TestingLogWriter{t})
			var resList []string
			err := c2.MtStreamedReplyWithSubjectParams(
				context.Background(),
				"arg1", "arg2",
				func(ctx context.Context, rep *SimpleStringReply) {
					resList = append(resList, rep.GetReply())
				})
			if err != nil {
				t.Fatal(err)
			}
			if resList[0] != "arg1" {
				t.Errorf("Expected 'arg1', got '%s'", resList[0])
			}
			if resList[1] != "arg2" {
				t.Errorf("Expected 'arg2', got '%s'", resList[1])
			}
		})

		t.Run("NoRequest method with params", func(t *testing.T) {
			log.SetOutput(TestingLogWriter{t})
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
			log.SetOutput(TestingLogWriter{t})
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
			for range []int{0, 1} {
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

	}
}

func expectsStringSlice(t *testing.T, expected, actual []string) {
	if len(expected) != len(actual) {
		t.Errorf("Expected %v, got %v", expected, actual)
		return
	}
	for i := range expected {
		if expected[i] != actual[i] {
			t.Errorf("Expected %v, got %v", expected, actual)
			return
		}
	}
}
