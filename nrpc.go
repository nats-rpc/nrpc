package nrpc

import (
	"bytes"
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

//go:generate protoc --go_out=../../.. nrpc.proto

type NatsConn interface {
	Request(subj string, data []byte, timeout time.Duration) (*nats.Msg, error)
	Publish(subj string, data []byte) error
	Subscribe(subj string, handler nats.MsgHandler) (*nats.Subscription, error)
	SubscribeSync(subj string) (*nats.Subscription, error)
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
