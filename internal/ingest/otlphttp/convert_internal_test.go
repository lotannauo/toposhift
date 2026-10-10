package otlphttp

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// A bytes value that is set is never nil in the model, whatever the protobuf
// message holds, because that is how a set empty value is told from an unset one.
func TestConvertSetEmptyBytes(t *testing.T) {
	v, err := convertValue(&commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: nil}})
	if err != nil {
		t.Fatal(err)
	}
	if v.BytesValue == nil || *v.BytesValue == nil {
		t.Error("a set bytes value converted to an unset one")
	}
}
