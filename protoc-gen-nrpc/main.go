package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/rapidloop/nrpc"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/golang/protobuf/protoc-gen-go/generator"
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

func goType(td *descriptor.FieldDescriptorProto) string {
	// Use protoc-gen-go generator to get the actual go type (for plain types
	// only!)
	t, _ := (*generator.Generator)(nil).GoType(nil, td)
	// We assume proto3, but pass nil to the generator, which will assume proto2.
	// The consequence is a leading star on the type that we need to trim
	return strings.TrimPrefix(t, "*")
}

// splitMessageTypeName split a message type into (package name, type name)
func splitMessageTypeName(name string) (string, string) {
	if len(name) == 0 {
		log.Fatal("Empty message type")
	}
	if name[0] != '.' {
		log.Fatalf("Expect message type name to start with '.', but it is '%s'", name)
	}
	lastDot := strings.LastIndex(name, ".")
	return name[1:lastDot], name[lastDot+1:]
}

func lookupFileDescriptor(name string) *descriptor.FileDescriptorProto {
	for _, fd := range request.GetProtoFile() {
		if fd.GetPackage() == name {
			return fd
		}
	}
	return nil
}

func lookupMessageType(name string) *descriptor.DescriptorProto {
	pkgname, msgname := splitMessageTypeName(name)
	fd := lookupFileDescriptor(pkgname)
	if fd == nil {
		log.Fatalf("Could not find the .proto file for package '%s'", pkgname)
	}
	for _, mt := range fd.GetMessageType() {
		if mt.GetName() == msgname {
			return mt
		}
	}
	return nil
}

func getField(d *descriptor.DescriptorProto, name string) *descriptor.FieldDescriptorProto {
	for _, f := range d.GetField() {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

func getOneofDecl(d *descriptor.DescriptorProto, name string) *descriptor.OneofDescriptorProto {
	for _, of := range d.GetOneofDecl() {
		if of.GetName() == name {
			return of
		}
	}
	return nil
}

// matchReply checks if a message matches the 'reply' pattern. If so, it returns
// the 'result' and 'error' fields.
func matchReply(d *descriptor.DescriptorProto) (result *descriptor.FieldDescriptorProto, err *descriptor.FieldDescriptorProto) {
	// Must have one and only one 'oneof', and its name must be 'reply'
	if len(d.GetOneofDecl()) != 1 {
		return
	}

	if d.GetOneofDecl()[0].GetName() != "reply" {
		return
	}
	// All fields must be part of the "reply" oneof, and be either result or error
	for _, field := range d.GetField() {
		switch field.GetName() {
		case "result":
			result = field
		case "error":
			err = field
		default:
			return nil, nil
		}
	}
	return
}

func pkgSubject(fd *descriptor.FileDescriptorProto) string {
	e, err := proto.GetExtension(fd.Options, nrpc.E_PackageSubject)
	if err == nil {
		value := e.(*string)
		return *value
	}
	return fd.GetPackage()
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
	"GetPkgSubjectPrefix": func(fd *descriptor.FileDescriptorProto) string {
		if s := pkgSubject(fd); s != "" {
			return s + "."
		}
		return ""
	},
	"GetPkgSubject": pkgSubject,
	"GetPkgSubjectParams": func(fd *descriptor.FileDescriptorProto) []string {
		e, err := proto.GetExtension(fd.Options, nrpc.E_PackageSubjectParams)
		if err == nil {
			value := e.([]string)
			return value
		}
		return nil
	},
	"Prometheus": func() bool {
		return pluginPrometheus
	},
	"HasFullReply": func(
		md *descriptor.MethodDescriptorProto,
	) bool {
		d := lookupMessageType(md.GetOutputType())
		resultField, _ := matchReply(d)
		return resultField != nil
	},
	"GetResultType": func(
		md *descriptor.MethodDescriptorProto,
	) string {
		d := lookupMessageType(md.GetOutputType())

		resultField, _ := matchReply(d)

		if resultField != nil {
			if resultField.GetTypeName() == "" {
				return goType(resultField)
			}
			return resultField.GetTypeName()
		}
		return md.GetOutputType()
	},
	"HasPointerResultType": func(
		md *descriptor.MethodDescriptorProto,
	) bool {
		d := lookupMessageType(md.GetOutputType())

		resultField, _ := matchReply(d)

		return resultField.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE
	},
}

var request plugin.CodeGeneratorRequest

func main() {

	log.SetPrefix("protoc-gen-nrpc: ")
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("error: reading input: %v", err)
	}

	var response plugin.CodeGeneratorResponse
	if err := proto.Unmarshal(data, &request); err != nil {
		log.Fatalf("error: parsing input proto: %v", err)
	}

	if len(request.GetFileToGenerate()) == 0 {
		log.Fatal("error: no files to generate")
	}

	pluginPrometheus = request.GetParameter() == "plugins=prometheus"

	tmpl, err := template.New(".").Funcs(funcMap).Parse(tFile)
	if err != nil {
		log.Fatal(err)
	}

	for _, name := range request.GetFileToGenerate() {
		var fd *descriptor.FileDescriptorProto
		for _, fd = range request.GetProtoFile() {
			if name == fd.GetName() {
				break
			}
		}
		if fd == nil {
			log.Fatalf("could not find the .proto file for %s", name)
		}

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
