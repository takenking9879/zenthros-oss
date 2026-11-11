// SPDX-License-Identifier: Apache-2.0

package envelope

import (
	"cmp"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"reflect"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/jobspec"
)

// Protocol is this build's fingerprint of the agent's wire: every message of
// the services the agent speaks (agentServices), field by field, and the job
// spec spec_json carries. An agent and a Gateway built from different wires do not talk: the
// Gateway refuses the agent at Register, before any command. Protobuf drops a
// field it does not know without a word, so an agent built from another wire
// would act on part of a command. A comment does not change it; a field, its number or its type
// does.
func Protocol() string { return protocol }

var protocol = fingerprintOf(agentpb.File_agent_proto, reflect.TypeFor[jobspec.JobSpec]())

// agentServices are the services the agent speaks: its control stream to
// the Gateway (Agent), the logs the controller streams from it (AgentLogs),
// and a Kubernetes backend's agent asking the Gateway for its nodes
// (NodeSupply).
var agentServices = []protoreflect.Name{"Agent", "AgentLogs", "NodeSupply"}

// fingerprintOf is the fingerprint of agentServices in file with spec as the
// job spec.
func fingerprintOf(file protoreflect.FileDescriptor, spec reflect.Type) string {
	h := sha256.New()
	seen := map[protoreflect.FullName]bool{}
	for _, service := range agentServices {
		methods := file.Services().ByName(service).Methods()
		for i := range methods.Len() {
			m := methods.Get(i)
			fmt.Fprintf(h, "rpc %s.%s(%s) %s;", service, m.Name(), m.Input().FullName(), m.Output().FullName())
			messageShape(h, m.Input(), seen)
			messageShape(h, m.Output(), seen)
		}
	}
	jsonShape(h, spec, "spec", map[reflect.Type]bool{})
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// messageShape writes each field of m and of the messages and enums it
// holds, once each.
func messageShape(h hash.Hash, m protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) {
	if seen[m.FullName()] {
		return
	}
	seen[m.FullName()] = true
	fields := m.Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		fmt.Fprintf(h, "%s.%s=%d %s %s;", m.FullName(), f.Name(), f.Number(), f.Cardinality(), f.Kind())
		switch {
		case f.Message() != nil:
			fmt.Fprintf(h, "%s;", f.Message().FullName())
			messageShape(h, f.Message(), seen)
		case f.Enum() != nil:
			values := f.Enum().Values()
			for j := range values.Len() {
				fmt.Fprintf(h, "%s=%d;", values.Get(j).Name(), values.Get(j).Number())
			}
		}
	}
}

var (
	jsonMarshaler = reflect.TypeFor[json.Marshaler]()
	textMarshaler = reflect.TypeFor[encoding.TextMarshaler]()
)

// jsonShape writes the JSON paths of t, each with its kind; a type that
// encodes itself is one value.
func jsonShape(h hash.Hash, t reflect.Type, path string, seen map[reflect.Type]bool) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if p := reflect.PointerTo(t); p.Implements(jsonMarshaler) || p.Implements(textMarshaler) {
		fmt.Fprintf(h, "%s:%s;", path, t)
		return
	}
	switch t.Kind() {
	case reflect.Struct:
		if seen[t] {
			fmt.Fprintf(h, "%s:%s;", path, t)
			return
		}
		seen[t] = true
		defer delete(seen, t)
		for i := range t.NumField() {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			switch {
			case !f.IsExported() || name == "-":
			case f.Anonymous && name == "":
				jsonShape(h, f.Type, path, seen)
			default:
				jsonShape(h, f.Type, path+"."+cmp.Or(name, f.Name), seen)
			}
		}
	case reflect.Slice, reflect.Array:
		jsonShape(h, t.Elem(), path+"[]", seen)
	case reflect.Map:
		jsonShape(h, t.Elem(), path+"{"+t.Key().Kind().String()+"}", seen)
	default:
		fmt.Fprintf(h, "%s:%s;", path, t.Kind())
	}
}
