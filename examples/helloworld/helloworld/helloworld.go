package helloworld

//go:generate protoc -I. -I../../.. --go_out . --go_opt=paths=source_relative --nrpc_out . --nrpc_opt=paths=source_relative helloworld.proto
