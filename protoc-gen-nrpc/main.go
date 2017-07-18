package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
)

// baseName returns the last path element of the name, with the last dotted suffix removed.
func baseName(name string) string {
	// First, find the last element
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	// Now drop the suffix
	if i := strings.LastIndex(name, "."); i >= 0 {
		name = name[0:i]
	}
	return name
}

// goPackageOption interprets the file's go_package option.
// If there is no go_package, it returns ("", "", false).
// If there's a simple name, it returns ("", pkg, true).
// If the option implies an import path, it returns (impPath, pkg, true).
func goPackageOption(d *descriptor.FileDescriptorProto) (impPath, pkg string, ok bool) {
	pkg = d.GetOptions().GetGoPackage()
	if pkg == "" {
		return
	}
	ok = true
	// The presence of a slash implies there's an import path.
	slash := strings.LastIndex(pkg, "/")
	if slash < 0 {
		return
	}
	impPath, pkg = pkg, pkg[slash+1:]
	// A semicolon-delimited suffix overrides the package name.
	sc := strings.IndexByte(impPath, ';')
	if sc < 0 {
		return
	}
	impPath, pkg = impPath[:sc], impPath[sc+1:]
	return
}

// goPackageName returns the Go package name to use in the
// generated Go file.  The result explicit reports whether the name
// came from an option go_package statement.  If explicit is false,
// the name was derived from the protocol buffer's package statement
// or the input file name.
func goPackageName(d *descriptor.FileDescriptorProto) (name string, explicit bool) {
	// Does the file have a "go_package" option?
	if _, pkg, ok := goPackageOption(d); ok {
		return pkg, true
	}

	// Does the file have a package clause?
	if pkg := d.GetPackage(); pkg != "" {
		return pkg, false
	}
	// Use the file base name.
	return baseName(d.GetName()), false
}

// goFileName returns the output name for the generated Go file.
func goFileName(d *descriptor.FileDescriptorProto) string {
	name := *d.Name
	if ext := path.Ext(name); ext == ".proto" || ext == ".protodevel" {
		name = name[:len(name)-len(ext)]
	}
	name += ".nrpc.go"

	// Does the file have a "go_package" option?
	// If it does, it may override the filename.
	if impPath, _, ok := goPackageOption(d); ok && impPath != "" {
		// Replace the existing dirname with the declared import path.
		_, name = path.Split(name)
		name = path.Join(impPath, name)
		return name
	}

	return name
}

var pluginPrometheus bool

var funcMap = template.FuncMap{
	"GoPackageName": func(fd *descriptor.FileDescriptorProto) string {
		p, _ := goPackageName(fd)
		return p
	},
	"GetPkg": func(pkg, s string) string {
		s = strings.TrimPrefix(s, ".")
		s = strings.TrimPrefix(s, pkg)
		s = strings.TrimPrefix(s, ".")
		return s
	},
	"Prometheus": func() bool {
		return pluginPrometheus
	},
}

func main() {

	log.SetPrefix("protoc-gen-nrpc: ")
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("error: reading input: %v", err)
	}

	var request plugin.CodeGeneratorRequest
	var response plugin.CodeGeneratorResponse
	if err := proto.Unmarshal(data, &request); err != nil {
		log.Fatalf("error: parsing input proto: %v", err)
	}

	if len(request.FileToGenerate) == 0 {
		log.Fatal("error: no files to generate")
	}

	pluginPrometheus = request.GetParameter() == "plugins=prometheus"

	tmpl, err := template.New(".").Funcs(funcMap).Parse(tFile)
	if err != nil {
		log.Fatal(err)
	}

	for _, fd := range request.ProtoFile {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, fd); err != nil {
			log.Fatal(err)
		}

		response.File = append(response.File, &plugin.CodeGeneratorResponse_File{
			Name:    proto.String(goFileName(fd)),
			Content: proto.String(buf.String()),
		})
	}

	if data, err = proto.Marshal(&response); err != nil {
		log.Fatalf("error: failed to marshal output proto: %v", err)
	}
	if _, err := os.Stdout.Write(data); err != nil {
		log.Fatalf("error: failed to write output proto: %v", err)
	}
}
