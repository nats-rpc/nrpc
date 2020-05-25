package helloworld

//go:generate protoc --go_out . --go_opt=paths=source_relative --nrpc_out plugins=prometheus:. --nrpc_opt=paths=source_relative helloworld.proto
