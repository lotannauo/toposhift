package mcphttp

import (
	"strings"
	"testing"
)

// The protocol's initialize answer carries the server's own instructions, and,
// when the configuration gives more, that text after one space. With none given
// the answer is the server's own text alone.
func TestInitializeCarriesTheConfiguredInstructionsAfterTheServersOwn(t *testing.T) {
	const extra = "This is a fixture. The history runs from one hour to the next."
	for _, tc := range []struct {
		name  string
		extra string
		want  string
	}{
		{"none", "", instructions},
		{"some", extra, instructions + " " + extra},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(Config{Version: "test", Instructions: tc.extra})
			if err != nil {
				t.Fatal(err)
			}
			cs := connect(t, s)
			res := cs.InitializeResult()
			if res == nil {
				t.Fatal("no initialize answer")
			}
			if res.Instructions != tc.want {
				t.Errorf("instructions %q, want %q", res.Instructions, tc.want)
			}
			if !strings.HasPrefix(res.Instructions, instructions) {
				t.Errorf("the server's own instructions are not first in %q", res.Instructions)
			}
		})
	}
}
