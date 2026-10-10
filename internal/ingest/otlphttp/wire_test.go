package otlphttp_test

import (
	"net/http"
	"reflect"
	"testing"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// exportRequestDescriptor describes ExportLogsServiceRequest as the OTLP
// collector's logs service defines it: one repeated field 1, resource_logs, of
// ResourceLogs. It is written out here because the package that defines it is
// not imported (it carries gRPC).
func exportRequestDescriptor(t testing.TB) protoreflect.MessageDescriptor {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("otlphttp_export_logs_test.proto"),
		Package:    proto.String("otlphttp.test"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{logspb.File_opentelemetry_proto_logs_v1_logs_proto.Path()},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("ExportLogsServiceRequest"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String("resource_logs"),
				JsonName: proto.String("resourceLogs"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".opentelemetry.proto.logs.v1.ResourceLogs"),
			}},
		}},
	}
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	return fd.Messages().ByName("ExportLogsServiceRequest")
}

// LogsData and ExportLogsServiceRequest are the same message on the wire: a
// single repeated field 1 of ResourceLogs. The handler decodes the first where
// the protocol sends the second, so this checks that a request encoded in the
// shape of the second, by hand and from a descriptor, is the same bytes as the
// first and decodes to the same value, in the handler as well.
func TestLogsDataIsWireCompatibleWithExportLogsServiceRequest(t *testing.T) {
	sample := sampleProto()

	// The field the two share, as the generated LogsData has it.
	f := (&logspb.LogsData{}).ProtoReflect().Descriptor().Fields().ByName("resource_logs")
	if f == nil || f.Number() != 1 || !f.IsList() || f.Message().FullName() != "opentelemetry.proto.logs.v1.ResourceLogs" {
		t.Fatalf("LogsData.resource_logs is not repeated ResourceLogs at field 1: %v", f)
	}

	asLogsData := marshalProto(t, sample)

	// By hand: field 1, length-delimited, once per ResourceLogs.
	var byHand []byte
	for _, rl := range sample.ResourceLogs {
		byHand = protowire.AppendTag(byHand, 1, protowire.BytesType)
		byHand = protowire.AppendBytes(byHand, marshalProto(t, rl))
	}

	// From a descriptor of the request message.
	desc := exportRequestDescriptor(t)
	req := dynamicpb.NewMessage(desc)
	list := req.Mutable(desc.Fields().ByName("resource_logs")).List()
	for _, rl := range sample.ResourceLogs {
		list.Append(protoreflect.ValueOfMessage(rl.ProtoReflect()))
	}
	asRequest := marshalProto(t, req)

	for name, b := range map[string][]byte{"by hand": byHand, "from a descriptor": asRequest} {
		if !reflect.DeepEqual(b, asLogsData) {
			t.Errorf("%s: the request bytes differ from the LogsData bytes", name)
		}
		var got logspb.LogsData
		if err := proto.Unmarshal(b, &got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !proto.Equal(&got, sample) {
			t.Errorf("%s: decoded as LogsData, the request is a different value", name)
		}
		h, sink := newHandler(otlphttpOptions())
		if rec := do(t, h, request{ctype: protoType, body: b}); rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d", name, rec.Code)
		}
		if len(sink.got) != 1 || !reflect.DeepEqual(sink.got[0], sampleModel()) {
			t.Errorf("%s: the sink did not receive the sample", name)
		}
	}

	// And the other way: bytes written as LogsData decode as the request.
	back := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(asLogsData, back); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(back, req) {
		t.Error("LogsData bytes decode to a different request")
	}
}
