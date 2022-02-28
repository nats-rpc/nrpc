package nrpc_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/T-J-L/nrpc"
)

//go:generate protoc --go_out=. --go_opt=paths=source_relative nrpc_test.proto
//go:generate mv nrpc_test.pb.go nrpcpb_test.go

func TestBasic(t *testing.T) {
	nc, err := nats.Connect(nrpc.NatsURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
		if err := nrpc.Publish(&DummyMessage{Foobar: "world"}, nil, nc, m.Reply, "protobuf"); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subn.Unsubscribe()

	var dm DummyMessage
	if err := nrpc.Call(
		&DummyMessage{Foobar: "hello"}, &dm, nc, "foo.bar", "protobuf", 5*time.Second,
	); err != nil {
		t.Fatal(err)
	}
	if dm.Foobar != "world" {
		t.Fatal("wrong response: ", string(dm.Foobar))
	}
}

func TestDecode(t *testing.T) {
	nc, err := nats.Connect(nrpc.NatsURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	var name string
	subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
		rname := strings.Split(m.Subject, ".")[1]
		var dm DummyMessage
		if rname != name {
			t.Fatal("unexpected name: " + rname)
		} else if err := proto.Unmarshal(m.Data, &dm); err != nil {
			t.Fatal(err)
		} else if dm.Foobar != "hello" {
			t.Fatal("unexpected inner request: " + dm.Foobar)
		} else if err := nrpc.Publish(&DummyMessage{Foobar: "world"}, nil, nc, m.Reply, "protobuf"); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subn.Unsubscribe()

	var names = []string{"lorem", "ipsum", "dolor"}
	for _, n := range names {
		name = n
		var dm DummyMessage
		if err := nrpc.Call(
			&DummyMessage{Foobar: "hello"}, &dm, nc, "foo."+name, "protobuf", 5*time.Second,
		); err != nil {
			t.Fatal(err)
		}
		if dm.Foobar != "world" {
			t.Fatal("wrong response: ", string(dm.Foobar))
		}
	}
}

func TestStreamCall(t *testing.T) {
	nc, err := nats.Connect(nrpc.NatsURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	t.Run("Simple stream", func(t *testing.T) {
		subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
			time.Sleep(60 * time.Millisecond)
			// Send an empty message
			if err := nc.Publish(m.Reply, []byte{0}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(60 * time.Millisecond)
			// Send a first message
			if err := nrpc.Publish(
				&DummyMessage{Foobar: "hello"},
				nil,
				nc, m.Reply, "protobuf",
			); err != nil {
				t.Fatal(err)
			}
			time.Sleep(60 * time.Millisecond)
			log.Print("Sending 'world'")
			// Send a second message
			if err := nrpc.Publish(
				&DummyMessage{Foobar: "world"},
				nil,
				nc, m.Reply, "protobuf",
			); err != nil {
				t.Fatal(err)
			}
			log.Print("Sending EOF")
			// Send the EOS marker
			if err := nrpc.Publish(
				nil,
				&nrpc.Error{Type: nrpc.Error_EOS, MsgCount: 2},
				nc, m.Reply, "protobuf",
			); err != nil {
				t.Fatal(err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer subn.Unsubscribe()

		sub, err := nrpc.StreamCall(
			context.TODO(), nc, "foo.*", &DummyMessage{}, "protobuf", 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}

		var dm DummyMessage
		var cont bool

		err = sub.Next(&dm)
		if err != nil {
			t.Fatal(err)
		}

		err = sub.Next(&dm)
		if err != nil {
			t.Fatal(err)
		}

		err = sub.Next(&dm)
		if err != nrpc.ErrEOS {
			t.Fatalf("Expected EOS, got %s", err)
		}
		if cont {
			t.Errorf("Expects cont=false")
		}
		err = sub.Next(&dm)
		if err != nats.ErrBadSubscription {
			t.Errorf("Expected a ErrBadSubscription, got %s", err)
		}
	})

	t.Run("Error", func(t *testing.T) {
		subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
			if err := nrpc.Publish(
				nil,
				&nrpc.Error{Type: nrpc.Error_CLIENT, Message: "error"},
				nc, m.Reply, "protobuf",
			); err != nil {
				t.Fatal(err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer subn.Unsubscribe()

		sub, err := nrpc.StreamCall(
			context.TODO(), nc, "foo.*", &DummyMessage{}, "protobuf", 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}

		err = sub.Next(&DummyMessage{})
		if err == nil {
			t.Fatalf("Expected an error, got %s", err)
		}
		if nErr, ok := err.(*nrpc.Error); ok {
			if nErr.Message != "error" {
				t.Errorf("Expected message='error', got %s", nErr.Message)
			}
		} else {
			t.Errorf("Expected a nrpc.Error, got %s", err)
		}
		err = sub.Next(&DummyMessage{})
		if err != nats.ErrBadSubscription {
			t.Errorf("Expected a ErrBadSubscription, got %s", err)
		}
	})
}

func TestError(t *testing.T) {
	nc, err := nats.Connect(nrpc.NatsURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
		if err := nrpc.Publish(
			&DummyMessage{Foobar: "world"},
			&nrpc.Error{Type: nrpc.Error_CLIENT, Message: "anerror"},
			nc, m.Reply, "protobuf",
		); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subn.Unsubscribe()

	err = nrpc.Call(&DummyMessage{Foobar: "hello"}, &DummyMessage{}, nc, "foo.bar", "protobuf", 5*time.Second)
	if err == nil {
		t.Fatal("error expected")
	}
	if err.Error() != "CLIENT error: anerror" {
		t.Fatal("wrong error: ", err.Error())
	}
}

func TestTimeout(t *testing.T) {
	nc, err := nats.Connect(nrpc.NatsURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
		time.Sleep(time.Second)
		if err := nrpc.Publish(&DummyMessage{Foobar: "world"}, nil, nc, m.Reply, "protobuf"); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subn.Unsubscribe()

	err = nrpc.Call(&DummyMessage{Foobar: "hello"}, &DummyMessage{}, nc, "foo.bar", "protobuf", 500*time.Millisecond)
	if err == nil {
		t.Fatal("error expected")
	} else if err.Error() != "nats: timeout" {
		t.Fatal("unexpected error: " + err.Error())
	}
}

var (
	encodingTestMsg = DummyMessage{Foobar: "hello"}
	encodingTests   = []struct {
		encoding string
		data     []byte
	}{
		{"protobuf", []byte{10, 5, 104, 101, 108, 108, 111}},
		{"json", []byte(`{"foobar":"hello"}`)},
	}
)

func TestMarshal(t *testing.T) {
	for _, tt := range encodingTests {
		t.Run("Marshal"+tt.encoding, func(t *testing.T) {
			b, err := nrpc.Marshal(tt.encoding, &encodingTestMsg)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(b, tt.data) {
				t.Errorf("Marshal %s failed", tt.encoding)
			}
		})
	}
}

func TestUnmarshal(t *testing.T) {
	for _, tt := range encodingTests {
		t.Run("Unmarshal"+tt.encoding, func(t *testing.T) {
			var msg DummyMessage
			err := nrpc.Unmarshal(tt.encoding, tt.data, &msg)
			if err != nil {
				t.Fatal(err)
			}
			if msg.Foobar != encodingTestMsg.Foobar {
				t.Errorf(
					"%s decode failed. Expected %#v, got %#v",
					tt.encoding, encodingTestMsg, msg)
			}
		})
	}
}

func TestMarshalUnmarshalResponse(t *testing.T) {
	for _, tt := range encodingTests {
		t.Run("UnmarshalResponse"+tt.encoding, func(t *testing.T) {
			var msg DummyMessage
			err := nrpc.UnmarshalResponse(tt.encoding, tt.data, &msg)
			if err != nil {
				t.Fatal(err)
			}
			if msg.Foobar != encodingTestMsg.Foobar {
				t.Errorf(
					"%s decode failed. Expected %#v, got %#v",
					tt.encoding, encodingTestMsg, msg)
			}
		})
		t.Run("MarshalErrorResponse"+tt.encoding, func(t *testing.T) {
			data, err := nrpc.MarshalErrorResponse(tt.encoding, &nrpc.Error{
				Type: nrpc.Error_SERVER, Message: "Some error",
			})
			if err != nil {
				t.Fatal("Unexpected error:", err)
			}
			switch tt.encoding {
			case "protobuf":
				if data[0] != 0 {
					t.Error("Expects payload to start with a '0', got", data[0])
				}
			case "json":
				var expected = `{"__error__":{"type":"SERVER","message":"Some error"}}`
				if string(data) != expected {
					t.Errorf("Invalid json-encoded error. Expects %s, got %s", expected, string(data))
				}
			}
			var msg DummyMessage
			err = nrpc.UnmarshalResponse(tt.encoding, data, &msg)
			if err == nil {
				t.Errorf("Expected an error, got nil")
				return
			}
			repErr, ok := err.(*nrpc.Error)
			if !ok {
				t.Errorf("Expected a nrpc.Error, got %#v", err)
				return
			}
			if repErr.Type != nrpc.Error_SERVER || repErr.Message != "Some error" {
				t.Errorf("Unexpected err: %#v", *repErr)
			}
		})
	}
}

// MSG Greeter.SayHello-json 1 _INBOX.test 16\r\n{"foobar":"hello"}\r\n

func compareStringSlices(t *testing.T, expected, actual []string) {
	if len(expected) != len(expected) {
		t.Errorf("String slices are different. Expected [%s], got [%s]",
			strings.Join(expected, ","), strings.Join(actual, ","))
		return
	}
	for i, expectedValue := range expected {
		if actual[i] != expectedValue {
			t.Errorf("String slices are different. Expected [%s], got [%s]",
				strings.Join(expected, ","), strings.Join(actual, ","))
			return
		}
	}
}

func TestParseSubject(t *testing.T) {
	for i, tt := range []struct {
		pkgSubject     string
		pkgParamsCount int
		svcSubject     string
		svcParamsCount int
		mtParamsCount  int
		subject        string
		pkgParams      []string
		svcParams      []string
		mtParams       []string
		name           string
		encoding       string
		err            string
	}{
		{"", 0, "foo", 0, 0, "foo.bar", nil, nil, nil, "bar", "protobuf", ""},
		{"", 0, "foo", 0, 0, "foo.bar.protobuf", nil, nil, nil, "bar", "protobuf", ""},
		{"", 0, "foo", 0, 0, "foo.bar.json", nil, nil, nil, "bar", "json", ""},
		{"", 0, "foo", 0, 0, "foo.bar.json.protobuf", nil, nil, nil, "bar", "",
			"Invalid subject tail length. Expects 0 or 1 parts, got 2"},
		{"demo", 0, "foo", 0, 0, "demo.foo.bar", nil, nil, nil, "bar", "protobuf", ""},
		{"demo", 0, "foo", 0, 0, "demo.foo.bar.json", nil, nil, nil, "bar", "json", ""},
		{"demo", 0, "foo", 0, 0, "foo.bar.json", nil, nil, nil, "", "",
			"Invalid subject prefix. Expected 'demo', got 'foo'"},
		{"demo", 2, "foo", 0, 0, "demo.p1.p2.foo.bar.json", []string{"p1", "p2"}, nil, nil, "bar", "json", ""},
		{"demo", 2, "foo", 1, 0, "demo.p1.p2.foo.sp1.bar.json", []string{"p1", "p2"}, []string{"sp1"}, nil, "bar", "json", ""},
		{"demo.pkg", 1, "nested.svc", 1, 0, "demo.pkg.p1.nested.svc.sp1.bar",
			[]string{"p1"}, []string{"sp1"}, nil, "bar", "protobuf", ""},
	} {
		pkgParams, svcParams, name, tail, err := nrpc.ParseSubject(
			tt.pkgSubject, tt.pkgParamsCount,
			tt.svcSubject, tt.svcParamsCount,
			tt.subject)
		var mtParams []string
		var encoding string
		if err == nil {
			mtParams, encoding, err = nrpc.ParseSubjectTail(tt.mtParamsCount, tail)
		}
		compareStringSlices(t, tt.pkgParams, pkgParams)
		compareStringSlices(t, tt.svcParams, svcParams)
		compareStringSlices(t, tt.mtParams, mtParams)
		if name != tt.name {
			t.Errorf("test %d: Expected name=%s, got %s", i, tt.name, name)
		}
		if encoding != tt.encoding {
			t.Errorf("text %d: Expected encoding=%s, got %s", i, tt.encoding, encoding)
		}
		if tt.err == "" && err != nil {
			t.Errorf("text %d: Unexpected error %s", i, err)
		} else if tt.err != "" && err == nil {
			t.Errorf("text %d: Expected error, got nothing", i)
		} else if tt.err != "" && tt.err != err.Error() {
			t.Errorf("text %d: Expected error '%s', got '%s'", i, tt.err, err)
		}
	}
}

func TestCaptureErrors(t *testing.T) {
	t.Run("NoError", func(t *testing.T) {
		msg, err := nrpc.CaptureErrors(func() (proto.Message, error) {
			return &DummyMessage{Foobar: "Hi"}, nil
		})
		if err != nil {
			t.Error("Unexpected error:", err)
		}
		dm, ok := msg.(*DummyMessage)
		if !ok {
			t.Error("Expected a DummyMessage, got", msg)
		}
		if dm.Foobar != "Hi" {
			t.Error("Message was not passed properly")
		}
	})
	t.Run("ClientError", func(t *testing.T) {
		msg, err := nrpc.CaptureErrors(func() (proto.Message, error) {
			return nil, fmt.Errorf("anerror")
		})
		if err == nil {
			t.Fatal("Expected an error, got nothing")
		}
		if err.Type != nrpc.Error_CLIENT {
			t.Errorf("Invalid error type. Expected 'CLIENT' (0), got %s", err.Type)
		}
		if err.Message != "anerror" {
			t.Error("Unexpected error message. Expected 'anerror', got", err.Message)
		}
		if msg != nil {
			t.Error("Expected a nil msg, got", msg)
		}
	})
	t.Run("DirectError", func(t *testing.T) {
		msg, err := nrpc.CaptureErrors(func() (proto.Message, error) {
			return nil, &nrpc.Error{Type: nrpc.Error_SERVER, Message: "anerror"}
		})
		if err == nil {
			t.Fatal("Expected an error, got nothing")
		}
		if err.Type != nrpc.Error_SERVER {
			t.Errorf("Invalid error type. Expected 'SERVER' (1), got %s", err.Type)
		}
		if err.Message != "anerror" {
			t.Error("Unexpected error message. Expected 'anerror', got", err.Message)
		}
		if msg != nil {
			t.Error("Expected a nil msg, got", msg)
		}
	})
	t.Run("ServerError", func(t *testing.T) {
		msg, err := nrpc.CaptureErrors(func() (proto.Message, error) {
			panic("panicking")
		})
		if err == nil {
			t.Fatal("Expected an error, got nothing")
		}
		if err.Type != nrpc.Error_SERVER {
			t.Errorf("Invalid error type. Expected 'SERVER' (1), got %s", err.Type)
		}
		if err.Message != "panicking" {
			t.Error("Unexpected error message. Expected 'panicking', got", err.Message)
		}
		if msg != nil {
			t.Error("Expected a nil msg, got", msg)
		}
	})
}
