package nrpc

import (
	"errors"
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/go-nats"
)

func Call(name string, req proto.Message, nc *nats.Conn, subject string, timeout time.Duration) (resp []byte, err error) {
	// encode request
	inner, err := proto.Marshal(req)
	if err != nil {
		log.Printf("nrpc: inner request marshal failed: %v", err)
		return
	}

	// encode rpc request
	request := RPCRequest{
		Name:    name,
		Request: inner,
	}
	rawRequest, err := proto.Marshal(&request)
	if err != nil {
		log.Printf("nrpc: request marshal failed: %v", err)
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

func Decode(req []byte) (name string, inner []byte, err error) {
	// decode the request
	var request RPCRequest
	if err = proto.Unmarshal(req, &request); err != nil {
		log.Printf("nrpc: request unmarshal failed: %v", err)
		return
	}

	name = request.Name
	inner = request.Request
	return
}

func Publish(resp proto.Message, errstr string, nc *nats.Conn, subject string) (err error) {
	// encode response
	var inner []byte
	if len(errstr) == 0 { // send any inner object only if error is unset
		inner, err = proto.Marshal(resp)
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
	rawResponse, err := proto.Marshal(&response)
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
