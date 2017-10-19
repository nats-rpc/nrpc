package nrpc

import (
	"bytes"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestHelloWorldExample(t *testing.T) {
	// make sure protoc-gen-nrpc is up to date
	installGenRPC := exec.Command("go", "install", "./protoc-gen-nrpc")
	if err := installGenRPC.Run(); err != nil {
		t.Fatal("Install protoc-gen-nrpc failed", err)
	}
	// generate the sources
	generate := exec.Command("go", "generate", "./examples/helloworld/helloworld")
	if err := generate.Run(); err != nil {
		t.Fatal("Generate failed", err)
	}
	// build
	buildServer := exec.Command("go", "build",
		"-o", "./examples/helloworld/greeter_server/greeter_server",
		"./examples/helloworld/greeter_server")
	if out, err := buildServer.CombinedOutput(); err != nil {
		t.Fatal("Buid server failed", err, string(out))
	}
	buildClient := exec.Command("go", "build",
		"-o", "./examples/helloworld/greeter_client/greeter_client",
		"./examples/helloworld/greeter_client")
	if out, err := buildClient.CombinedOutput(); err != nil {
		t.Fatal("Buid client failed", err, string(out))
	}
	// run the server
	server := exec.Command("./examples/helloworld/greeter_server/greeter_server")
	var serverStdout bytes.Buffer
	server.Stdout = &serverStdout
	server.Start()
	defer func() {
		if server.Process != nil {
			server.Process.Signal(os.Interrupt)
		}
		if err := server.Wait(); err != nil {
			t.Error("Server run failed:", err)
			t.Error("Server output:", serverStdout.String())
		}
	}()

	// run the client and check its output
	client := exec.Command("./examples/helloworld/greeter_client/greeter_client")
	timeout := time.AfterFunc(time.Second, func() { client.Process.Kill() })
	out, err := client.CombinedOutput()
	timeout.Stop()
	if err != nil {
		t.Fatal("Run client failed with:", err)
	}
	expectedOuput := "Greeting: Hello world\n"
	if string(out) != expectedOuput {
		t.Errorf("Wrong client output. Expected '%s', got '%s'",
			expectedOuput, string(out))
	}
}
