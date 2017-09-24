package helloworld

//go:generate protoc --go_out . --nrpc_out plugins=prometheus:. helloworld.proto
