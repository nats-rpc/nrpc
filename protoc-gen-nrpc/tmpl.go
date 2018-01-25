package main

const tFile = `// This code was autogenerated from {{.GetName}}, do not edit.

{{- $pkgName := GoPackageName .}}
{{- $pkgSubject := GetPkgSubject .}}
{{- $pkgSubjectPrefix := GetPkgSubjectPrefix .}}
{{- $pkgSubjectParams := GetPkgSubjectParams .}}
package {{$pkgName}}

import (
	"context"
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/go-nats"
	{{- range  GetExtraImports .}}
	{{.}}
	{{- end}}
	{{- if Prometheus}}
	"github.com/prometheus/client_golang/prometheus"
	{{- end}}
	"github.com/rapidloop/nrpc"
)

{{- range .Service}}

// {{.GetName}}Server is the interface that providers of the service
// {{.GetName}} should implement.
type {{.GetName}}Server interface {
	{{- range .Method}}
	{{- if ne .GetInputType ".nrpc.NoRequest"}}
	{{- $resultType := GetResultType .}}
	{{.GetName}}(ctx context.Context
		{{- range GetMethodSubjectParams . -}}
		, {{ . }} string
		{{- end -}}
		{{- if ne .GetInputType ".nrpc.Void" -}}
		, req {{GoType .GetInputType}}
		{{- end -}}
		{{- if HasStreamedReply . -}}
		, pushRep func({{GoType .GetOutputType}})
		{{- end -}}
	)
		{{- if ne $resultType ".nrpc.NoReply" }} (
		{{- if and (ne $resultType ".nrpc.Void") (not (HasStreamedReply .)) -}}
		resp *{{GoType $resultType}}, {{end -}}
		err error)
		{{- end -}}
	{{- end}}
	{{- end}}
}

{{- if Prometheus}}

var (
	// The request completion time, measured at client-side.
	clientRCTFor{{.GetName}} = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "nrpc_client_request_completion_time_seconds",
			Help:       "The request completion time for calls, measured client-side.",
			Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			ConstLabels: map[string]string{
				"service": "{{.GetName}}",
			},
		},
		[]string{"method"})

	// The handler execution time, measured at server-side.
	serverHETFor{{.GetName}} = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "nrpc_server_handler_execution_time_seconds",
			Help:       "The handler execution time for calls, measured server-side.",
			Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			ConstLabels: map[string]string{
				"service": "{{.GetName}}",
			},
		},
		[]string{"method"})

	// The counts of calls made by the client, classified by result type.
	clientCallsFor{{.GetName}} = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nrpc_client_calls_count",
			Help: "The count of calls made by the client.",
			ConstLabels: map[string]string{
				"service": "{{.GetName}}",
			},
		},
		[]string{"method", "encoding", "result_type"})

	// The counts of requests handled by the server, classified by result type.
	serverRequestsFor{{.GetName}} = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nrpc_server_requests_count",
			Help: "The count of requests handled by the server.",
			ConstLabels: map[string]string{
				"service": "{{.GetName}}",
			},
		},
		[]string{"method", "encoding", "result_type"})
)
{{- end}}

// {{.GetName}}Handler provides a NATS subscription handler that can serve a
// subscription using a given {{.GetName}}Server implementation.
type {{.GetName}}Handler struct {
	ctx    context.Context
	nc     nrpc.NatsConn
	server {{.GetName}}Server
}

func New{{.GetName}}Handler(ctx context.Context, nc nrpc.NatsConn, s {{.GetName}}Server) *{{.GetName}}Handler {
	return &{{.GetName}}Handler{
		ctx:    ctx,
		nc:     nc,
		server: s,
	}
}

func (h *{{.GetName}}Handler) Subject() string {
	return "{{$pkgSubjectPrefix}}
	{{- range $pkgSubjectParams -}}
		*.
	{{- end -}}
	{{GetServiceSubject .}}
	{{- range GetServiceSubjectParams . -}}
		.*
	{{- end -}}
	.>"
}
{{- $serviceName := .GetName}}
{{- $serviceSubject := GetServiceSubject .}}
{{- $serviceSubjectParams := GetServiceSubjectParams .}}
{{- range .Method}}
{{- if eq .GetInputType ".nrpc.NoRequest"}}

func (h *{{$serviceName}}Handler) {{.GetName}}Publish(
	{{- range $pkgSubjectParams}}pkg{{.}} string, {{end -}}
	{{- range $serviceSubjectParams}}svc{{.}} string, {{end -}}
	{{- range GetMethodSubjectParams .}}mt{{.}} string, {{end -}}
	msg {{GoType .GetOutputType}}) error {
	rawMsg, err := nrpc.Marshal("protobuf", &msg)
	if err != nil {
		log.Printf("{{$serviceName}}Handler.{{.GetName}}Publish: error marshaling the message: %s", err)
		return err
	}
	subject := "{{$pkgSubject}}."
	{{- range $pkgSubjectParams}} + pkg{{.}} + "."{{end -}}
	+ "{{$serviceSubject}}."
	{{- range $serviceSubjectParams}} + svc{{.}} + "."{{end -}}
	+ "{{GetMethodSubject .}}"
	{{- range GetMethodSubjectParams .}} + "." + mt{{.}}{{end}}
	return h.nc.Publish(subject, rawMsg)
}
{{- end}}
{{- if HasStreamedReply .}}

func (h *{{$serviceName}}Handler) {{.GetName}}Handler(ctx context.Context, tail []string, msg *nats.Msg) {
	_, encoding, err := nrpc.ParseSubjectTail({{len (GetMethodSubjectParams .)}}, tail)
	if err != nil {
		log.Printf("{{$serviceName}}: {{.GetName}} subject parsing failed:")
	}

	{{- if ne .GetInputType ".nrpc.Void"}}
	var req {{GoType .GetInputType}}
	if err := nrpc.Unmarshal(encoding, msg.Data, &req); err != nil {
		// Handle error
		return
	}
	{{- end}}

	ctx, cancel := context.WithCancel(ctx)

	keepStreamAlive := nrpc.NewKeepStreamAlive(h.nc, msg.Reply, encoding, cancel)

	var msgCount uint32

	_, nrpcErr := nrpc.CaptureErrors(func() (proto.Message, error) {
		err := h.server.{{.GetName}}(ctx
		{{- range GetMethodSubjectParams . -}}
		, {{.}}
		{{- end -}}
		{{- if ne .GetInputType ".nrpc.Void" -}}
		, req
		{{- end -}}
		, func(rep {{GoType .GetOutputType}}){
				if err = nrpc.Publish(&rep, nil, h.nc, msg.Reply, encoding); err != nil {
					log.Printf("nrpc: error publishing response")
					cancel()
					return
				}
				msgCount++
			})
		return nil, err
	})
	keepStreamAlive.Stop()

	if nrpcErr != nil {
		nrpc.Publish(nil, nrpcErr, h.nc, msg.Reply, encoding)
	} else {
		nrpc.Publish(
			nil, &nrpc.Error{Type: nrpc.Error_EOS, MsgCount: msgCount},
			h.nc, msg.Reply, encoding)
	}
}
{{- end}}
{{- end}}

{{- if ServiceNeedsHandler .}}

func (h *{{.GetName}}Handler) Handler(msg *nats.Msg) {
	var encoding string
	var noreply bool
	// extract method name & encoding from subject
	{{ if ne 0 (len $pkgSubjectParams)}}pkgParams{{else}}_{{end -}},
	{{- if ne 0 (len (GetServiceSubjectParams .))}} svcParams{{else}} _{{end -}}
	, name, tail, err := nrpc.ParseSubject(
		"{{$pkgSubject}}", {{len $pkgSubjectParams}}, "{{GetServiceSubject .}}", {{len (GetServiceSubjectParams .)}}, msg.Subject)
	if err != nil {
		log.Printf("{{.GetName}}Hanlder: {{.GetName}} subject parsing failed: %v", err)
		return
	}

	ctx := h.ctx
	{{- range $i, $name := $pkgSubjectParams }}
	ctx = context.WithValue(ctx, "nrpc-pkg-{{$name}}", pkgParams[{{$i}}])
	{{- end }}
	{{- range $i, $name := GetServiceSubjectParams . }}
	ctx = context.WithValue(ctx, "nrpc-svc-{{$name}}", svcParams[{{$i}}])
	{{- end }}
	// call handler and form response
	var resp proto.Message
	var replyError *nrpc.Error
{{- if Prometheus}}
	var elapsed float64
{{- end}}
	switch name {
	{{- range .Method}}
	case "{{GetMethodSubject .}}":
		{{- if eq .GetInputType ".nrpc.NoRequest"}}
		// {{.GetName}} is a no-request method. Ignore it.
		return
		{{- else if HasStreamedReply .}}
		h.{{.GetName}}Handler(ctx, tail, msg)
		return
		{{- else}}{{/* HasStreamedReply */}}
		{{- if ne 0 (len (GetMethodSubjectParams .))}}
		var mtParams []string
		{{- end}}
		{{- if eq .GetOutputType ".nrpc.NoReply"}}
		noreply = true
		{{- end}}
		{{if eq 0 (len (GetMethodSubjectParams .))}}_{{else}}mtParams{{end}}, encoding, err = nrpc.ParseSubjectTail({{len (GetMethodSubjectParams .)}}, tail)
		if err != nil {
			log.Printf("{{.GetName}}Hanlder: {{.GetName}} subject parsing failed: %v", err)
			break
		}
		var req {{GoType .GetInputType}}
		if err := nrpc.Unmarshal(encoding, msg.Data, &req); err != nil {
			log.Printf("{{.GetName}}Handler: {{.GetName}} request unmarshal failed: %v", err)
			replyError = &nrpc.Error{
				Type: nrpc.Error_CLIENT,
				Message: "bad request received: " + err.Error(),
			}
{{- if Prometheus}}
			serverRequestsFor{{$serviceName}}.WithLabelValues(
				"{{.GetName}}", encoding, "unmarshal_fail").Inc()
{{- end}}
		} else {
{{- if Prometheus}}
			start := time.Now()
{{- end}}
			resp, replyError = nrpc.CaptureErrors(
				func()(proto.Message, error){
					{{- if eq .GetOutputType ".nrpc.NoReply" -}}
					var innerResp nrpc.NoReply
					{{else}}
					{{if eq .GetOutputType ".nrpc.Void" -}}
					var innerResp nrpc.Void
					{{else}}innerResp, {{end -}}
					err := {{end -}}
					h.server.{{.GetName}}(ctx
					{{- range $i, $p := GetMethodSubjectParams . -}}
					, mtParams[{{ $i }}]
					{{- end -}}
					{{- if ne .GetInputType ".nrpc.Void" -}}
					, req
					{{- end -}}
					)
					if err != nil {
						return nil, err
					}
					return innerResp, err
				})
{{- if Prometheus}}
			elapsed = time.Since(start).Seconds()
{{- end}}
			if replyError != nil {
				log.Printf("{{.GetName}}Handler: {{.GetName}} handler failed: %s", replyError.Error())
{{- if Prometheus}}
				serverRequestsFor{{$serviceName}}.WithLabelValues(
					"{{.GetName}}", encoding, "handler_fail").Inc()
{{- end}}
			}
		}
		{{- end}}{{/* not HasStreamedReply */}}
{{- end}}{{/* range .Method */}}
	default:
		log.Printf("{{.GetName}}Handler: unknown name %q", name)
		replyError = &nrpc.Error{
			Type: nrpc.Error_CLIENT,
			Message: "unknown name: " + name,
		}
{{- if Prometheus}}
		serverRequestsFor{{.GetName}}.WithLabelValues(
			"{{.GetName}}", encoding, "name_fail").Inc()
{{- end}}
	}


	if !noreply {
		// encode and send response
		err = nrpc.Publish(resp, replyError, h.nc, msg.Reply, encoding) // error is logged
	} else {
		err = nil
	}
{{- if Prometheus}}
	if err != nil {
		serverRequestsFor{{$serviceName}}.WithLabelValues(
			name, encoding, "sendreply_fail").Inc()
	} else if replyError == nil {
		serverRequestsFor{{$serviceName}}.WithLabelValues(
			name, encoding, "success").Inc()
	}

	// report metric to Prometheus
	serverHETFor{{$serviceName}}.WithLabelValues(name).Observe(elapsed)
{{- else}}
	if err != nil {
		log.Println("{{.GetName}}Handler: {{.GetName}} handler failed to publish the response: %s", err)
	}
{{- end}}
}
{{- end}}

type {{.GetName}}Client struct {
	nc      nrpc.NatsConn
	{{- if ne 0 (len $pkgSubject)}}
	PkgSubject string
	{{- end}}
	{{- range $pkgSubjectParams}}
	PkgParam{{ . }} string
	{{- end}}
	Subject string
	{{- range GetServiceSubjectParams .}}
	SvcParam{{ . }} string
	{{- end}}
	Encoding string
	Timeout time.Duration
}

func New{{.GetName}}Client(nc nrpc.NatsConn
	{{- range $pkgSubjectParams -}}
	, pkgParam{{.}} string
	{{- end -}}
	{{- range GetServiceSubjectParams . -}}
	, svcParam{{ . }} string
	{{- end -}}
	) *{{.GetName}}Client {
	return &{{.GetName}}Client{
		nc:      nc,
		PkgSubject: "{{$pkgSubject}}",
		{{- range $pkgSubjectParams}}
		PkgParam{{.}}: pkgParam{{.}},
		{{- end}}
		Subject: "{{GetServiceSubject .}}",
		{{- range GetServiceSubjectParams .}}
		SvcParam{{.}}: svcParam{{.}},
		{{- end}}
		Encoding: "protobuf",
		Timeout: 5 * time.Second,
	}
}
{{- $serviceName := .GetName}}
{{- $serviceSubjectParams := GetServiceSubjectParams .}}
{{- range .Method}}
{{- $resultType := GetResultType .}}
{{- if eq .GetInputType ".nrpc.NoRequest"}}

func (c *{{$serviceName}}Client) {{.GetName}}Subject(
	{{range GetMethodSubjectParams .}}mt{{.}} string,{{end}}
) string {
	return {{ if ne 0 (len $pkgSubject) -}}
		c.PkgSubject + "." + {{end}}
	{{- range $pkgSubjectParams -}}
		c.PkgParam{{.}} + "." + {{end -}}
	c.Subject + "." + {{range $serviceSubjectParams -}}
		c.SvcParam{{.}} + "." + {{end -}}
	"{{GetMethodSubject .}}"
	{{- range GetMethodSubjectParams .}} + "." + mt{{.}}{{end}}
}

type {{$serviceName}}{{.GetName}}Subscription struct {
	*nats.Subscription
}

func (s *{{$serviceName}}{{.GetName}}Subscription) Next(timeout time.Duration) (next {{GoType .GetOutputType}}, err error) {
	msg, err := s.Subscription.NextMsg(timeout)
	if err != nil {
		return
	}
	err = nrpc.Unmarshal("protobuf", msg.Data, &next)
	return
}

func (c *{{$serviceName}}Client) {{.GetName}}SubscribeSync(
	{{range GetMethodSubjectParams .}}mt{{.}} string,{{end}}
) (sub *{{$serviceName}}{{.GetName}}Subscription, err error) {
	subject := c.{{.GetName}}Subject(
		{{range GetMethodSubjectParams .}}mt{{.}},{{end}}
	)
	natsSub, err := c.nc.SubscribeSync(subject)
	if err != nil {
		return
	}
	sub = &{{$serviceName}}{{.GetName}}Subscription{natsSub}
	return
}

func (c *{{$serviceName}}Client) {{.GetName}}Subscribe(
	{{range GetMethodSubjectParams .}}mt{{.}} string,{{end}}
	handler func ({{GoType .GetOutputType}}),
) (sub *nats.Subscription, err error) {
	subject := c.{{.GetName}}Subject(
		{{range GetMethodSubjectParams .}}mt{{.}},{{end}}
	)
	sub, err = c.nc.Subscribe(subject, func(msg *nats.Msg){
		var pmsg {{GoType .GetOutputType}}
		err := nrpc.Unmarshal("protobuf", msg.Data, &pmsg)
		if err != nil {
			log.Printf("{{$serviceName}}Client.{{.GetName}}Subscribe: Error decoding, %s", err)
			return
		}
		handler(pmsg)
	})
	return
}

func (c *{{$serviceName}}Client) {{.GetName}}SubscribeChan(
	{{range GetMethodSubjectParams .}}mt{{.}} string,{{end}}
) (<-chan {{GoType .GetOutputType}}, *nats.Subscription, error) {
	ch := make(chan {{GoType .GetOutputType}})
	sub, err := c.{{.GetName}}Subscribe(
		{{- range GetMethodSubjectParams .}}mt{{.}}, {{end -}}
		func (msg {{GoType .GetOutputType}}) {
		ch <- msg
	})
	return ch, sub, err
}

{{- else if HasStreamedReply .}}

func (c *{{$serviceName}}Client) {{.GetName}}(
	ctx context.Context,
	{{- range GetMethodSubjectParams . -}}
	{{ . }} string,
	{{- end}}
	{{- if ne .GetInputType ".nrpc.Void"}}
	req {{GoType .GetInputType}},
	{{- end}}
	cb func (context.Context, {{GoType .GetOutputType}}),
) error {
{{- if Prometheus}}
	start := time.Now()
{{- end}}
	subject := {{ if ne 0 (len $pkgSubject) -}}
		c.PkgSubject + "." + {{end}}
	{{- range $pkgSubjectParams -}}
		c.PkgParam{{.}} + "." + {{end -}}
	c.Subject + "." + {{range $serviceSubjectParams -}}
		c.SvcParam{{.}} + "." + {{end -}}
	"{{GetMethodSubject .}}"
	{{- range GetMethodSubjectParams . }} + "." + {{ . }}{{ end -}}
	;

	sub, err := nrpc.StreamCall(ctx, c.nc, subject
		{{- if ne .GetInputType ".nrpc.Void" -}}
		, &req
		{{- else -}}
		, &nrpc.Void{}
		{{- end -}}
		, c.Encoding, c.Timeout)
	if err != nil {
		{{- if Prometheus}}
		clientCallsFor{{$serviceName}}.WithLabelValues(
			"{{.GetName}}", c.Encoding, "error").Inc()
		{{- end}}
		return err
	}

	var res {{GoType .GetOutputType}}
	for {
		err = sub.Next(&res)
		if err != nil {
			break
		}
		cb(ctx, res)
	}
	if err == nrpc.ErrEOS {
		err = nil
	}
{{- if Prometheus}}
	// report total time taken to Prometheus
	elapsed := time.Since(start).Seconds()
	clientRCTFor{{$serviceName}}.WithLabelValues("{{.GetName}}").Observe(elapsed)
	clientCallsFor{{$serviceName}}.WithLabelValues(
		"{{.GetName}}", c.Encoding, "success").Inc()
{{- end}}
	return err
}
{{- else}}

func (c *{{$serviceName}}Client) {{.GetName}}(
	{{- range GetMethodSubjectParams . -}}
	{{ . }} string, {{ end -}}
	{{- if ne .GetInputType ".nrpc.Void" -}}
	req {{GoType .GetInputType}}
	{{- end -}}) (
		{{- if not (eq $resultType ".nrpc.Void" ".nrpc.NoReply") -}}
		resp *{{GoType $resultType}}, {{end -}}
		err error) {
{{- if Prometheus}}
	start := time.Now()
{{- end}}

	subject := {{ if ne 0 (len $pkgSubject) -}}
		c.PkgSubject + "." + {{end}}
	{{- range $pkgSubjectParams -}}
		c.PkgParam{{.}} + "." + {{end -}}
	c.Subject + "." + {{range $serviceSubjectParams -}}
		c.SvcParam{{.}} + "." + {{end -}}
	"{{GetMethodSubject .}}"
	{{- range GetMethodSubjectParams . }} + "." + {{ . }}{{ end -}}
	;

	// call
	{{- if eq .GetInputType ".nrpc.Void"}}
	var req {{GoType .GetInputType}}
	{{- end}}
	{{- if eq .GetOutputType ".nrpc.Void" ".nrpc.NoReply"}}
	var resp {{GoType .GetOutputType}}
	{{- end}}
	err = nrpc.Call(&req, resp, c.nc, subject, c.Encoding, c.Timeout)
	if err != nil {
{{- if Prometheus}}
		clientCallsFor{{$serviceName}}.WithLabelValues(
			"{{.GetName}}", c.Encoding, "call_fail").Inc()
{{- end}}
		return // already logged
	}

{{- if Prometheus}}

	// report total time taken to Prometheus
	elapsed := time.Since(start).Seconds()
	clientRCTFor{{$serviceName}}.WithLabelValues("{{.GetName}}").Observe(elapsed)
	clientCallsFor{{$serviceName}}.WithLabelValues(
		"{{.GetName}}", c.Encoding, "success").Inc()
{{- end}}

	return
}
{{- end}}
{{- end}}
{{- end}}

type Client struct {
	nc      nrpc.NatsConn
	defaultEncoding string
	defaultTimeout time.Duration
	{{- if ne 0 (len $pkgSubject)}}
	pkgSubject string
	{{- end}}
	{{- range $pkgSubjectParams}}
	pkgParam{{ . }} string
	{{- end}}

	{{- range .Service}}
	{{.GetName}} *{{.GetName}}Client
	{{- end}}
}

func NewClient(nc nrpc.NatsConn
	{{- range $pkgSubjectParams -}}
	, pkgParam{{.}} string
	{{- end -}}) *Client {
	c := Client{
		nc: nc,
		defaultEncoding: "protobuf",
		defaultTimeout: 5*time.Second,
		pkgSubject: "{{$pkgSubject}}",
		{{- range $pkgSubjectParams}}
		pkgParam{{.}}: pkgParam{{.}},
		{{- end}}
	};
	{{- range .Service}}
	{{- if eq 0 (len (GetServiceSubjectParams .))}}
	c.{{.GetName}} = New{{.GetName}}Client(nc
	{{- range $pkgSubjectParams -}}
		, c.pkgParam{{ . }}
	{{- end}})
	{{- end}}
	{{- end}}
	return &c
}

func (c *Client) SetEncoding(encoding string) {
	c.defaultEncoding = encoding
	{{- range .Service}}
	if c.{{.GetName}} != nil {
		c.{{.GetName}}.Encoding = encoding
	}
	{{- end}}
}

func (c *Client) SetTimeout(t time.Duration) {
	c.defaultTimeout = t
	{{- range .Service}}
	if c.{{.GetName}} != nil {
		c.{{.GetName}}.Timeout = t
	}
	{{- end}}
}

{{- range .Service}}
{{- if ne 0 (len (GetServiceSubjectParams .))}}

func (c *Client) Set{{.GetName}}Params(
	{{- range GetServiceSubjectParams .}}
	{{ . }} string,
	{{- end}}
) {
	c.{{.GetName}} = New{{.GetName}}Client(
		c.nc,
	{{- range $pkgSubjectParams}}
		c.pkgParam{{ . }},
	{{- end}}
	{{- range GetServiceSubjectParams .}}
		{{ . }},
	{{- end}}
	)
	c.{{.GetName}}.Encoding = c.defaultEncoding
	c.{{.GetName}}.Timeout = c.defaultTimeout
}

func (c *Client) New{{.GetName}}(
	{{- range GetServiceSubjectParams .}}
	{{ . }} string,
	{{- end}}
) *{{.GetName}}Client {
	client := New{{.GetName}}Client(
		c.nc,
	{{- range $pkgSubjectParams}}
		c.pkgParam{{ . }},
	{{- end}}
	{{- range GetServiceSubjectParams .}}
		{{ . }},
	{{- end}}
	)
	client.Encoding = c.defaultEncoding
	client.Timeout = c.defaultTimeout
	return client
}
{{- end}}
{{- end}}

{{- if Prometheus}}

func init() {
{{- range .Service}}
	// register metrics for service {{.GetName}}
	prometheus.MustRegister(clientRCTFor{{.GetName}})
	prometheus.MustRegister(serverHETFor{{.GetName}})
	prometheus.MustRegister(clientCallsFor{{.GetName}})
	prometheus.MustRegister(serverRequestsFor{{.GetName}})
{{- end}}
}
{{- end}}`
