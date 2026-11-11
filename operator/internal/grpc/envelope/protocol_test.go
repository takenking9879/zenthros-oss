// SPDX-License-Identifier: Apache-2.0

package envelope

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/jobspec"
)

// The fingerprint follows what the services the agent speaks carry, field
// by field, and the job spec's JSON: a field's number, its type, or a new
// field in a message one of their rpcs carries changes it; a message none
// carries does not. An agent built before such a change is refused at
// Register.
func TestTheProtocolFollowsTheAgentsWire(t *testing.T) {
	spec := reflect.TypeFor[jobspec.JobSpec]()
	if got := fingerprintOf(edited(t, nil), spec); got != Protocol() {
		t.Fatalf("the fingerprint of agent.proto as built is %s, Protocol says %s", got, Protocol())
	}
	for _, tc := range []struct {
		name    string
		edit    func(*descriptorpb.FileDescriptorProto)
		changes bool
	}{
		{"a field's number", func(f *descriptorpb.FileDescriptorProto) {
			field(t, f, "Advertise", "backend_id").Number = proto.Int32(99)
		}, true},
		{"a field's type", func(f *descriptorpb.FileDescriptorProto) {
			field(t, f, "Advertise", "session_epoch").Type = descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
		}, true},
		{"a new field in a command", func(f *descriptorpb.FileDescriptorProto) {
			submit := message(t, f, "SubmitRequest")
			submit.Field = append(submit.Field, newField("deadline", 99))
		}, true},
		{"the logs the controller streams from the agent come as another message", func(f *descriptorpb.FileDescriptorProto) {
			stream := method(t, f, "AgentLogs", "StreamLogs")
			stream.OutputType = stream.InputType
		}, true},
		{"a new field in the nodes a Kubernetes backend's agent asks for", func(f *descriptorpb.FileDescriptorProto) {
			slots := message(t, f, "NodeSlotsResponse")
			slots.Field = append(slots.Field, newField("zone", 99))
		}, true},
		{"a new field where no rpc of the agent goes", func(f *descriptorpb.FileDescriptorProto) {
			admin := message(t, f, "DeleteBackendRequest")
			admin.Field = append(admin.Field, newField("reason", 99))
		}, false},
	} {
		got := fingerprintOf(edited(t, tc.edit), spec)
		if changed := got != Protocol(); changed != tc.changes {
			t.Errorf("%s: fingerprint changed %v, want %v", tc.name, changed, tc.changes)
		}
	}
	type withAField struct {
		jobspec.JobSpec
		Deadline string `json:"deadline"`
	}
	if fingerprintOf(edited(t, nil), reflect.TypeFor[withAField]()) == Protocol() {
		t.Error("a new field of the job spec left the fingerprint as it was")
	}
}

// method is the rpc name of service in f.
func method(t *testing.T, f *descriptorpb.FileDescriptorProto, service, name string) *descriptorpb.MethodDescriptorProto {
	t.Helper()
	for _, s := range f.Service {
		if s.GetName() != service {
			continue
		}
		for _, m := range s.Method {
			if m.GetName() == name {
				return m
			}
		}
	}
	t.Fatalf("agent.proto has no rpc %s.%s", service, name)
	return nil
}

// edited is agent.proto as built, with edit applied.
func edited(t *testing.T, edit func(*descriptorpb.FileDescriptorProto)) protoreflect.FileDescriptor {
	t.Helper()
	f := protodesc.ToFileDescriptorProto(agentpb.File_agent_proto)
	if edit != nil {
		edit(f)
	}
	file, err := protodesc.NewFile(f, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("the edited agent.proto: %v", err)
	}
	return file
}

func message(t *testing.T, f *descriptorpb.FileDescriptorProto, name string) *descriptorpb.DescriptorProto {
	t.Helper()
	for _, m := range f.MessageType {
		if m.GetName() == name {
			return m
		}
	}
	t.Fatalf("agent.proto has no message %s", name)
	return nil
}

func field(t *testing.T, f *descriptorpb.FileDescriptorProto, msg, name string) *descriptorpb.FieldDescriptorProto {
	t.Helper()
	for _, fd := range message(t, f, msg).Field {
		if fd.GetName() == name {
			return fd
		}
	}
	t.Fatalf("%s has no field %s", msg, name)
	return nil
}

func newField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name: proto.String(name), JsonName: proto.String(name), Number: proto.Int32(number),
		Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
	}
}
