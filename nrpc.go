package nrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/go-nats"
)

//go:generate protoc --go_out=../../.. nrpc.proto

type Reply interface {
	proto.Message
	GetError() *Error
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

// ExtractFunctionNameAndEncoding parses a subject and extract the function
// name and the encoding (defaults to "protobuf").
// The subject structure is: "[package.]service.function[-encoding]"
func ExtractFunctionNameAndEncoding(subject string) (name string, encoding string, err error) {
	dotSplitted := strings.Split(subject, ".")
	if len(dotSplitted) == 2 {
		name = dotSplitted[1]
		encoding = "protobuf"
	} else if len(dotSplitted) == 3 {
		name = dotSplitted[1]
		encoding = dotSplitted[2]
	} else {
		err = errors.New(
			"Invalid subject. Expects 2 or 3 parts, got " + subject,
		)
	}

	return
}

func Call(req proto.Message, rep proto.Message, nc *nats.Conn, subject string, encoding string, timeout time.Duration) error {
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
	msg, err := nc.Request(subject, rawRequest, timeout)
	if err != nil {
		log.Printf("nrpc: nats request failed: %v", err)
		return err
	}

	data := msg.Data

	// If the reply type is not a Reply implementation, if will be encapsulated
	// in a RPCResponse
	if _, ok := rep.(Reply); !ok {
		var response RPCResponse
		if err = Unmarshal(encoding, data, &response); err != nil {
			log.Printf("nrpc: response unmarshal failed: %v", err)
			return err
		}
		if response.GetError() != nil {
			return response.GetError()
		}
		data = response.GetResult()
	}

	// decode rpc reponse
	if err := Unmarshal(encoding, data, rep); err != nil {
		log.Printf("nrpc: response unmarshal failed: %v", err)
		return err
	}

	if reply, ok := rep.(Reply); ok && reply.GetError() != nil {
		return reply.GetError()
	}

	return nil
}

func Publish(resp proto.Message, withError *Error, nc *nats.Conn, subject string, encoding string) error {
	if _, ok := resp.(Reply); !ok {
		// wrap the response in a RPCResponse
		if withError == nil { // send any inner object only if error is unset
			inner, err := Marshal(encoding, resp)
			if err != nil {
				log.Printf("nrpc: inner response marshal failed: %v", err)
				// Don't return here. Send back a response to the caller.
				withError = &Error{
					Type:    Error_SERVER,
					Message: "nrpc: inner response marshal failed server-side",
				}
			} else {
				resp = &RPCResponse{
					Reply: &RPCResponse_Result{
						Result: inner,
					},
				}
			}
		}
		if withError != nil {
			resp = &RPCResponse{
				Reply: &RPCResponse_Error{
					Error: withError,
				},
			}
		}
	}

	rawResponse, err := Marshal(encoding, resp)
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
