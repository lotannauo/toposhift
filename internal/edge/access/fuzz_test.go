package access

import (
	"context"
	"strings"
	"testing"
)

// FuzzVerify holds that no input panics and that the only input the verifier
// admits is the one canonical assertion it was given a key for: every other
// encoding of it, and every edit of it, is refused.
func FuzzVerify(f *testing.F) {
	e := newEnv(f)
	good := e.token(f, nil)
	parts := strings.Split(good, ".")
	for _, seed := range []string{
		good, "", ".", "..", "a.b.c", parts[0] + "." + parts[1] + ".",
		parts[0] + "." + parts[1] + "." + parts[2] + "=",
		parts[0] + "=." + parts[1] + "." + parts[2],
		e.token(f, func(h, _ map[string]any) { h["alg"] = "none" }),
		e.token(f, func(_, p map[string]any) { p["aud"] = "x" }),
		strings.Repeat("a.", 100),
	} {
		f.Add(seed)
	}
	// Load the key once so that no iteration depends on the fetch.
	if _, err := e.verify(good); err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, tok string) {
		id, err := e.v.Verify(context.Background(), tok)
		if err != nil {
			if id != (Identity{}) {
				t.Fatalf("a refusal returned %+v", id)
			}
			return
		}
		if tok != good {
			t.Fatalf("admitted %q, which is not the assertion that was signed", tok)
		}
	})
}
