package nrpc

import (
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/go-nats"
)

func TestBasic(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
		if err := Publish(&DummyMessage{"world"}, "", nc, m.Reply); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subn.Unsubscribe()

	resp, err := Call(&DummyMessage{"hello"}, nc, "foo.bar", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var dm DummyMessage
	if err := proto.Unmarshal(resp, &dm); err != nil {
		t.Fatal(err)
	} else if dm.Foobar != "world" {
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
		} else if err := Publish(&DummyMessage{"world"}, "", nc, m.Reply); err != nil {
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
		resp, err := Call(&DummyMessage{"hello"}, nc, "foo."+name, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var dm DummyMessage
		if err := proto.Unmarshal(resp, &dm); err != nil {
			t.Fatal(err)
		} else if dm.Foobar != "world" {
			t.Fatal("wrong response: ", string(dm.Foobar))
		}
	}
}

func TestError(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	subn, err := nc.Subscribe("foo.*", func(m *nats.Msg) {
		if err := Publish(&DummyMessage{"world"}, "anerror", nc, m.Reply); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subn.Unsubscribe()

	_, err = Call(&DummyMessage{"hello"}, nc, "foo.bar", 5*time.Second)
	if err == nil {
		t.Fatal("error expected")
	}
	if err.Error() != "anerror" {
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
		if err := Publish(&DummyMessage{"world"}, "", nc, m.Reply); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subn.Unsubscribe()

	_, err = Call(&DummyMessage{"hello"}, nc, "foo.bar", 500*time.Millisecond)
	if err == nil {
		t.Fatal("error expected")
	} else if err.Error() != "nats: timeout" {
		t.Fatal("unexpected error: " + err.Error())
	}
}

//------------------------------------------------------------------------------

type DummyMessage struct {
	Foobar string `protobuf:"bytes,1,opt,name=foobar" json:"foobar,omitempty"`
}

func (m *DummyMessage) Reset()                    { *m = DummyMessage{} }
func (m *DummyMessage) String() string            { return proto.CompactTextString(m) }
func (*DummyMessage) ProtoMessage()               {}
func (*DummyMessage) Descriptor() ([]byte, []int) { return dummyfileDescriptor0, []int{0} }

func init() {
	proto.RegisterType((*DummyMessage)(nil), "DummyMessage")
}

var dummyfileDescriptor0 = []byte{
	// 76 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xe2, 0xe2, 0xca, 0x2b, 0x2a, 0x48,
	0xd6, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x57, 0x52, 0xe3, 0xe2, 0x71, 0x29, 0xcd, 0xcd, 0xad, 0xf4,
	0x4d, 0x2d, 0x2e, 0x4e, 0x4c, 0x4f, 0x15, 0x12, 0xe3, 0x62, 0x4b, 0xcb, 0xcf, 0x4f, 0x4a, 0x2c,
	0x92, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x0c, 0x82, 0xf2, 0x92, 0xd8, 0xc0, 0xca, 0x8d, 0x01, 0x01,
	0x00, 0x00, 0xff, 0xff, 0x76, 0x6f, 0x42, 0xc1, 0x3c, 0x00, 0x00, 0x00,
}
