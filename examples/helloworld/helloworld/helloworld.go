package helloworld

//go:generate protoc -I. -I ../../../../../.. --go_out ../../../../../.. --nrpc_out ../../../../../.. helloworld.proto
