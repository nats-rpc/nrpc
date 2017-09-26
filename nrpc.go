package nrpc

import (
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/go-nats"
)

func Unmarshal(encoding string, data []byte, msg proto.Message) error {
	switch encoding {
	case "proto":
		return proto.Unmarshal(data, msg)
	case "json":
		return json.Unmarshal(data, msg)
	default:
		return errors.New("Invalid encoding: " + encoding)
	}
}

func Marshal(encoding string, msg proto.Message) ([]byte, error) {
	switch encoding {
	case "proto":
		return proto.Marshal(msg)
	case "json":
		return json.Marshal(msg)
	default:
		return nil, errors.New("Invalid encoding: " + encoding)
	}
}

// ExtractFunctionNameAndEncoding parses a subject and extract the function
// name and the encoding (defaults to "proto").
// The subject structure is: "[package.]service.function[-encoding]"
func ExtractFunctionNameAndEncoding(subject string) (name string, encoding string, err error) {
	dotSplitted := strings.Split(subject, ".")
	if len(dotSplitted) == 2 {
		name = dotSplitted[1]
		encoding = "proto"
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

func Call(req proto.Message, nc *nats.Conn, subject string, timeout time.Duration) (resp []byte, err error) {
	// encode request
	rawRequest, err := proto.Marshal(req)
	if err != nil {
		log.Printf("nrpc: inner request marshal failed: %v", err)
		return
	}

	// call
	msg, err := nc.Request(subject, rawRequest, timeout)
	if err != nil {
		log.Printf("nrpc: nats request failed: %v", err)
		return
	}

	// decode rpc reponse
	var response RPCResponse
	if err = proto.Unmarshal(msg.Data, &response); err != nil {
		log.Printf("nrpc: response unmarshal failed: %v", err)
		return
	}

	// check error
	if len(response.Error) > 0 {
		err = errors.New(response.Error)
		log.Printf("nrpc: handler failed: %s", response.Error)
		return
	}

	resp = response.Response
	return
}

func Publish(resp proto.Message, errstr string, nc *nats.Conn, subject string, encoding string) (err error) {
	// encode response
	var inner []byte
	if len(errstr) == 0 { // send any inner object only if error is unset
		inner, err = Marshal(encoding, resp)
		if err != nil {
			log.Printf("nrpc: inner response marshal failed: %v", err)
			// Don't return here. Send back a response to the caller.
			errstr = "nrpc: inner response marshal failed server-side"
			inner = nil
		}
	}

	// encode rpc response
	response := RPCResponse{
		Error:    errstr,
		Response: inner,
	}
	rawResponse, err := Marshal(encoding, &response)
	if err != nil {
		log.Printf("nrpc: rpc response marshal failed: %v", err)
		return
	}

	// send response
	if err = nc.Publish(subject, rawResponse); err != nil {
		log.Printf("nrpc: response publish failed: %v", err)
	}

	return nil
}
