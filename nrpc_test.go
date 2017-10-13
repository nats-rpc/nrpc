package nrpc_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/go-nats"

	"github.com/rapidloop/nrpc"
)

//go:generate protoc --go_out=. nrpc_test.proto
//go:generate mv nrpc_test.pb.go nrpcpb_test.go

func TestBasic(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
		if err := nrpc.Publish(&DummyMessage{"world"}, "", nc, m.Reply, "protobuf"); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subn.Unsubscribe()

	var dm DummyMessage
	if err := nrpc.Call(
		&DummyMessage{"hello"}, &dm, nc, "foo.bar", "protobuf", 5*time.Second,
	); err != nil {
		t.Fatal(err)
	}
	if dm.Foobar != "world" {
		t.Fatal("wrong response: ", string(dm.Foobar))
	}
}

func TestDecode(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(5*time.Second))
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
		} else if err := nrpc.Publish(&DummyMessage{"world"}, "", nc, m.Reply, "protobuf"); err != nil {
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
			&DummyMessage{"hello"}, &dm, nc, "foo."+name, "protobuf", 5*time.Second,
		); err != nil {
			t.Fatal(err)
		}
		if dm.Foobar != "world" {
			t.Fatal("wrong response: ", string(dm.Foobar))
		}
	}
}

func TestReply(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	subn, err := nc.Subscribe("foo", func(m *nats.Msg) {
		var (
			dm DummyMessage
		)
		if err := nrpc.Unmarshal("protobuf", m.Data, &dm); err != nil {
			t.Fatal(err)
		}
		var dr DummyReply
		if dm.Foobar == "Hi" {
			dr.Reply = &DummyReply_Foobar{
				Foobar: dm.Foobar,
			}
		} else {
			dr.Reply = &DummyReply_Error{
				Error: &nrpc.Error{
					Type:    nrpc.Error_CLIENT,
					Message: "You did not say Hi",
				},
			}
		}
		if err := nrpc.Publish(&dr, "", nc, m.Reply, "protobuf"); err != nil {
			t.Fatal(err)
		}
	})
	defer subn.Unsubscribe()

	t.Run("Publish", func(t *testing.T) {

		data, err := nrpc.Marshal("protobuf", &DummyMessage{"Hi"})
		if err != nil {
			t.Fatal(err)
		}
		reply, err := nc.Request("foo", data, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var dr DummyReply
		if err := nrpc.Unmarshal("protobuf", reply.Data, &dr); err != nil {
			t.Fatal(err)
		}
		if dr.GetError() != nil {
			t.Fatal("Got error:", dr.GetError())
		}
		if dr.GetFoobar() != "Hi" {
			t.Fatal("Shoud receive 'Hi', got", dr.GetFoobar())
		}
	})

	t.Run("Call", func(t *testing.T) {
		var (
			dm = DummyMessage{"Hi"}
			dr DummyReply
		)
		if err := nrpc.Call(&dm, &dr, nc, "foo", "protobuf", 5*time.Second); err != nil {
			t.Fatal(err)
		}
		if dr.GetError() != nil {
			t.Error("Unexpected error:", dr.GetError())
		}
		if dr.GetFoobar() != "Hi" {
			t.Error("Should get 'Hi', got", dr.GetFoobar())
		}
	})

	t.Run("Call with Error", func(t *testing.T) {
		var (
			dm = DummyMessage{"Not Hi"}
			dr DummyReply
		)
		err := nrpc.Call(&dm, &dr, nc, "foo", "protobuf", 5*time.Second)
		if err == nil {
			t.Fatal("Should be an error, got none")
		}
		e, isError := err.(*nrpc.Error)
		if !isError {
			t.Fatal("err should be a *nrpc.Error, but is", e)
		}
		if e.GetMessage() != "You did not say Hi" {
			t.Error("Error message should be 'You did not say Hi', got", e.GetMessage())
		}
	})
}

func TestError(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
		if err := nrpc.Publish(&DummyMessage{"world"}, "anerror", nc, m.Reply, "protobuf"); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subn.Unsubscribe()

	err = nrpc.Call(&DummyMessage{"hello"}, &DummyMessage{}, nc, "foo.bar", "protobuf", 5*time.Second)
	if err == nil {
		t.Fatal("error expected")
	}
	if err.Error() != "CLIENT error: anerror" {
		t.Fatal("wrong error: ", err.Error())
	}
}

func TestTimeout(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
		time.Sleep(time.Second)
		if err := nrpc.Publish(&DummyMessage{"world"}, "", nc, m.Reply, "protobuf"); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subn.Unsubscribe()

	err = nrpc.Call(&DummyMessage{"hello"}, &DummyMessage{}, nc, "foo.bar", "protobuf", 500*time.Millisecond)
	if err == nil {
		t.Fatal("error expected")
	} else if err.Error() != "nats: timeout" {
		t.Fatal("unexpected error: " + err.Error())
	}
}

var (
	encodingTestMsg = DummyMessage{"hello"}
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
			if msg != encodingTestMsg {
				t.Errorf(
					"Json decode failed. Expected %#v, got %#v",
					encodingTestMsg, msg)
			}
		})
	}
}

// MSG Greeter.SayHello-json 1 _INBOX.test 16\r\n{"foobar":"hello"}\r\n

func TestExtractFunctionNameAndEncoding(t *testing.T) {
	for _, tt := range []struct {
		subject  string
		name     string
		encoding string
		err      string
	}{
		{"foo.bar", "bar", "protobuf", ""},
		{"foo.bar.protobuf", "bar", "protobuf", ""},
		{"foo.bar.json", "bar", "json", ""},
		{"foo.bar.json.protobuf", "", "",
			"Invalid subject. Expects 2 or 3 parts, got foo.bar.json.protobuf"},
	} {
		name, encoding, err := nrpc.ExtractFunctionNameAndEncoding(tt.subject)
		if name != tt.name {
			t.Errorf("Expected name=%s, got %s", tt.name, name)
		}
		if encoding != tt.encoding {
			t.Errorf("Expected encoding=%s, got %s", tt.encoding, encoding)
		}
		if tt.err == "" && err != nil {
			t.Errorf("Unexpected error %s", err)
		} else if tt.err != "" && err == nil {
			t.Errorf("Expected error, got nothing")
		} else if tt.err != "" && tt.err != err.Error() {
			t.Errorf("Expected error '%s', got '%s'", tt.err, err)
		}
	}
}
