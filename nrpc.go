package nrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/go-nats"
)

// ErrStreamInvalidMsgCount is when a stream reply gets a wrong number of messages
var ErrStreamInvalidMsgCount = errors.New("Stream reply received an incorrect number of messages")

//go:generate protoc -I. -I../../.. --gogo_out=Mgoogle/protobuf/descriptor.proto=github.com/gogo/protobuf/protoc-gen-gogo/descriptor:../../.. nrpc.proto

type NatsConn interface {
	Publish(subj string, data []byte) error
	PublishRequest(subj, reply string, data []byte) error
	Request(subj string, data []byte, timeout time.Duration) (*nats.Msg, error)

	ChanSubscribe(subj string, ch chan *nats.Msg) (*nats.Subscription, error)
	Subscribe(subj string, handler nats.MsgHandler) (*nats.Subscription, error)
	SubscribeSync(subj string) (*nats.Subscription, error)
}

// ReplyInboxMaker returns a new inbox subject for a given nats connection.
type ReplyInboxMaker func(NatsConn) string

// GetReplyInbox is used by StreamCall to get a inbox subject
// It can be changed by a client lib that needs custom inbox subjects
var GetReplyInbox ReplyInboxMaker = func(NatsConn) string {
	return nats.NewInbox()
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s error: %s", Error_Type_name[int32(e.Type)], e.Message)
}

func Unmarshal(encoding string, data []byte, msg proto.Message) error {
	switch encoding {
	case "protobuf":
		return proto.Unmarshal(data, msg)
	case "json":
		return json.Unmarshal(data, msg)
	default:
		return errors.New("Invalid encoding: " + encoding)
	}
}

func UnmarshalResponse(encoding string, data []byte, msg proto.Message) error {
	switch encoding {
	case "protobuf":
		if len(data) > 0 && data[0] == 0 {
			var repErr Error
			if err := proto.Unmarshal(data[1:], &repErr); err != nil {
				return err
			}
			return &repErr
		}
		return proto.Unmarshal(data, msg)
	case "json":
		if len(data) > 13 && bytes.Equal(data[:13], []byte("{\"__error__\":")) {
			var rep map[string]*Error
			if err := json.Unmarshal(data, &rep); err != nil {
				return err
			}
			return rep["__error__"]
		}
		return json.Unmarshal(data, msg)
	default:
		return errors.New("Invalid encoding: " + encoding)
	}
}

func Marshal(encoding string, msg proto.Message) ([]byte, error) {
	switch encoding {
	case "protobuf":
		return proto.Marshal(msg)
	case "json":
		return json.Marshal(msg)
	default:
		return nil, errors.New("Invalid encoding: " + encoding)
	}
}

func MarshalErrorResponse(encoding string, repErr *Error) ([]byte, error) {
	switch encoding {
	case "protobuf":
		var (
			buf  = []byte{0}
			pBuf = proto.NewBuffer(buf)
		)
		if err := pBuf.Marshal(repErr); err != nil {
			return nil, err
		}
		return pBuf.Bytes(), nil
	case "json":
		return json.Marshal(map[string]*Error{
			"__error__": repErr,
		})
	default:
		return nil, errors.New("Invalid encoding: " + encoding)
	}
}

func ParseSubject(
	packageSubject string, packageParamsCount int,
	serviceSubject string, serviceParamsCount int,
	subject string,
) (packageParams []string, serviceParams []string,
	name string, tail []string, err error,
) {
	packageSubjectDepth := 0
	if packageSubject != "" {
		packageSubjectDepth = strings.Count(packageSubject, ".") + 1
	}
	serviceSubjectDepth := strings.Count(serviceSubject, ".") + 1
	subjectMinSize := packageSubjectDepth + packageParamsCount + serviceSubjectDepth + serviceParamsCount + 1

	tokens := strings.Split(subject, ".")
	if len(tokens) < subjectMinSize {
		err = fmt.Errorf(
			"Invalid subject len. Expects number of parts >= %d, got %d",
			subjectMinSize, len(tokens))
		return
	}
	if packageSubject != "" {
		for i, packageSubjectPart := range strings.Split(packageSubject, ".") {
			if tokens[i] != packageSubjectPart {
				err = fmt.Errorf(
					"Invalid subject prefix. Expected '%s', got '%s'",
					packageSubjectPart, tokens[i])
				return
			}
		}
		tokens = tokens[packageSubjectDepth:]
	}

	packageParams = tokens[0:packageParamsCount]
	tokens = tokens[packageParamsCount:]

	for i, serviceSubjectPart := range strings.Split(serviceSubject, ".") {
		if tokens[i] != serviceSubjectPart {
			err = fmt.Errorf(
				"Invalid subject. Service should be '%s', got '%s'",
				serviceSubjectPart, tokens[i])
			return
		}
	}
	tokens = tokens[serviceSubjectDepth:]

	serviceParams = tokens[0:serviceParamsCount]
	tokens = tokens[serviceParamsCount:]

	name = tokens[0]
	tokens = tokens[1:]

	tail = tokens
	return
}

func ParseSubjectTail(
	methodParamsCount int,
	tail []string,
) (
	methodParams []string, encoding string, err error,
) {
	if len(tail) < methodParamsCount || len(tail) > methodParamsCount+1 {
		err = fmt.Errorf(
			"Invalid subject tail length. Expects %d or %d parts, got %d",
			methodParamsCount, methodParamsCount+1, len(tail),
		)
		return
	}
	methodParams = tail[:methodParamsCount]
	tail = tail[methodParamsCount:]
	switch len(tail) {
	case 0:
		encoding = "protobuf"
	case 1:
		encoding = tail[0]
	default:
		panic("Got extra tokens, which should be impossible at this point")
	}
	return
}

func Call(req proto.Message, rep proto.Message, nc NatsConn, subject string, encoding string, timeout time.Duration) error {
	// encode request
	rawRequest, err := Marshal(encoding, req)
	if err != nil {
		log.Printf("nrpc: inner request marshal failed: %v", err)
		return err
	}

	if encoding != "protobuf" {
		subject += "." + encoding
	}

	// call
	if _, noreply := rep.(*NoReply); noreply {
		err := nc.Publish(subject, rawRequest)
		if err != nil {
			log.Printf("nrpc: nats publish failed: %v", err)
		}
		return err
	}
	msg, err := nc.Request(subject, rawRequest, timeout)
	if err != nil {
		log.Printf("nrpc: nats request failed: %v", err)
		return err
	}

	data := msg.Data

	if err := UnmarshalResponse(encoding, data, rep); err != nil {
		if _, isError := err.(*Error); !isError {
			log.Printf("nrpc: response unmarshal failed: %v", err)
		}
		return err
	}

	return nil
}

var ErrEOS = errors.New("End of stream")
var ErrCanceled = errors.New("Call canceled")

func NewStreamCallSubscription(
	ctx context.Context, nc NatsConn, encoding string, subject string,
	timeout time.Duration,
) (*StreamCallSubscription, error) {
	sub := StreamCallSubscription{
		ctx:      ctx,
		nc:       nc,
		encoding: encoding,
		subject:  subject,
		timeout:  timeout,
		timeoutT: time.NewTimer(timeout),
		subCh:    make(chan *nats.Msg, 256),
		nextCh:   make(chan *nats.Msg),
		quit:     make(chan struct{}),
		errCh:    make(chan error),
	}
	ssub, err := nc.ChanSubscribe(subject, sub.subCh)
	if err != nil {
		return nil, err
	}
	sub.sub = ssub
	go sub.loop()
	return &sub, nil
}

type StreamCallSubscription struct {
	ctx      context.Context
	nc       NatsConn
	encoding string
	subject  string
	timeout  time.Duration
	timeoutT *time.Timer
	sub      *nats.Subscription
	subCh    chan *nats.Msg
	nextCh   chan *nats.Msg
	quit     chan struct{}
	errCh    chan error
	msgCount uint32
}

func (sub *StreamCallSubscription) stop() {
	close(sub.quit)
}

func (sub *StreamCallSubscription) loop() {
	hbSubject := sub.subject + ".heartbeat"
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer sub.sub.Unsubscribe()
	for {
		select {
		case msg := <-sub.subCh:
			sub.timeoutT.Reset(sub.timeout)

			if len(msg.Data) == 1 && msg.Data[0] == 0 {
				break
			}

			sub.nextCh <- msg
		case <-sub.timeoutT.C:
			sub.errCh <- nats.ErrTimeout
			return
		case <-sub.ctx.Done():
			// send a 'lastbeat' and quit
			b, err := Marshal(sub.encoding, &HeartBeat{Lastbeat: true})
			if err != nil {
				err = fmt.Errorf("Error marshaling heartbeat: %s", err)
				sub.errCh <- err
				return
			}
			if err := sub.nc.Publish(hbSubject, b); err != nil {
				err = fmt.Errorf("Error sending heartbeat: %s", err)
				sub.errCh <- err
				return
			}
			sub.errCh <- ErrCanceled
			return
		case <-ticker.C:
			if err := sub.nc.Publish(hbSubject, nil); err != nil {
				err = fmt.Errorf("Error sending heartbeat: %s", err)
				sub.errCh <- err
				return
			}
		case <-sub.quit:
			return
		}
	}
}

func (sub *StreamCallSubscription) Next(rep proto.Message) error {
	if sub.sub == nil {
		return nats.ErrBadSubscription
	}
	select {
	case err := <-sub.errCh:
		sub.sub = nil
		return err
	case msg := <-sub.nextCh:
		if err := UnmarshalResponse(sub.encoding, msg.Data, rep); err != nil {
			sub.stop()
			sub.sub = nil
			if nrpcErr, ok := err.(*Error); ok {
				if nrpcErr.GetMsgCount() != sub.msgCount {
					log.Printf(
						"nrpc: received invalid number of messages. Expected %d, got %d",
						nrpcErr.GetMsgCount(), sub.msgCount)
				}
				if nrpcErr.GetType() == Error_EOS {
					if nrpcErr.GetMsgCount() != sub.msgCount {
						return ErrStreamInvalidMsgCount
					}
					return ErrEOS
				}
			} else {
				log.Printf("nrpc: response unmarshal failed: %v", err)
			}
			return err
		}
		sub.msgCount++
	}

	return nil
}

func StreamCall(ctx context.Context, nc NatsConn, subject string, req proto.Message, encoding string, timeout time.Duration) (*StreamCallSubscription, error) {
	rawRequest, err := Marshal(encoding, req)
	if err != nil {
		log.Printf("nrpc: inner request marshal failed: %v", err)
		return nil, err
	}

	if encoding != "protobuf" {
		subject += "." + encoding
	}

	reply := GetReplyInbox(nc)

	streamSub, err := NewStreamCallSubscription(ctx, nc, encoding, reply, timeout)
	if err != nil {
		return nil, err
	}

	if err := nc.PublishRequest(subject, reply, rawRequest); err != nil {
		streamSub.stop()
		return nil, err
	}
	return streamSub, nil
}

func Publish(resp proto.Message, withError *Error, nc NatsConn, subject string, encoding string) error {
	var rawResponse []byte
	var err error

	if withError != nil {
		rawResponse, err = MarshalErrorResponse(encoding, withError)
	} else {
		rawResponse, err = Marshal(encoding, resp)
	}

	if err != nil {
		log.Printf("nrpc: rpc response marshal failed: %v", err)
		return err
	}

	// send response
	if err := nc.Publish(subject, rawResponse); err != nil {
		log.Printf("nrpc: response publish failed: %v", err)
		return err
	}

	return nil
}

// CaptureErrors runs a handler and convert error and panics into proper Error
func CaptureErrors(fn func() (proto.Message, error)) (msg proto.Message, replyError *Error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Caught panic: %s\n%s", r, debug.Stack())
			replyError = &Error{
				Type:    Error_SERVER,
				Message: fmt.Sprint(r),
			}
		}
	}()
	var err error
	msg, err = fn()
	if err != nil {
		var ok bool
		if replyError, ok = err.(*Error); !ok {
			replyError = &Error{
				Type:    Error_CLIENT,
				Message: err.Error(),
			}
		}
	}
	return
}

func NewKeepStreamAlive(nc NatsConn, subject string, encoding string, onError func()) *KeepStreamAlive {
	k := KeepStreamAlive{
		nc:       nc,
		subject:  subject,
		encoding: encoding,
		c:        make(chan struct{}),
		onError:  onError,
	}
	go k.loop()
	return &k
}

type KeepStreamAlive struct {
	nc       NatsConn
	subject  string
	encoding string
	c        chan struct{}
	onError  func()
}

func (k *KeepStreamAlive) Stop() {
	close(k.c)
}

func (k *KeepStreamAlive) loop() {
	hbChan := make(chan *nats.Msg, 256)
	hbSub, err := k.nc.ChanSubscribe(k.subject+".heartbeat", hbChan)
	if err != nil {
		log.Printf("nrpc: could not subscribe to heartbeat: %s", err)
		k.onError()
	}
	defer func() {
		if err := hbSub.Unsubscribe(); err != nil {
			log.Printf("nrpc: error unsubscribing from heartbeat: %s", err)
		}
	}()
	hbDelay := 0
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case msg := <-hbChan:
			var hb HeartBeat
			log.Printf("nrpc: (%s.heartbeat) received %v", k.subject, msg)
			if err := Unmarshal(k.encoding, msg.Data, &hb); err != nil {
				log.Printf("nrpc: error unmarshaling heartbeat: %s", err)
				ticker.Stop()
				k.onError()
				return
			}
			if hb.Lastbeat {
				log.Printf("nrpc: client canceled the streamed reply. (%s)", k.subject)
				ticker.Stop()
				k.onError()
				return
			}
			hbDelay = 0
		case <-ticker.C:
			hbDelay++
			if hbDelay >= 5 {
				log.Printf("nrpc: No heartbeat received in 5 seconds. Canceling.")
				ticker.Stop()
				k.onError()
				return
			}
			if err := k.nc.Publish(k.subject, []byte{0}); err != nil {
				log.Printf("nrpc: error publishing response: %s", err)
				ticker.Stop()
				k.onError()
				return
			}
		case <-k.c:
			ticker.Stop()
			return
		}
	}
}
