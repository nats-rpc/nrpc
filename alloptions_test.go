package nrpc

import (
	"os/exec"
	"testing"
)

func TestAllOptionsExample(t *testing.T) {
	// make sure protoc-gen-nrpc is up to date
	installGenRPC := exec.Command("go", "install", "./protoc-gen-nrpc")
	if out, err := installGenRPC.CombinedOutput(); err != nil {
		t.Fatal("Install protoc-gen-nrpc failed", err, ":\n", string(out))
	}
	// generate the sources
	generate := exec.Command("go", "generate", "./examples/alloptions")
	if out, err := generate.CombinedOutput(); err != nil {
		t.Fatal("Generate failed", err, ":\n", string(out))
	}
	// build
	build := exec.Command("go", "build",
		"-o", "./examples/alloptions/alloptions",
		"./examples/alloptions")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatal("Build failed", err, string(out))
	}
}
