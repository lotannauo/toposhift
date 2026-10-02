package identity_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

func TestParseFingerprintRejects(t *testing.T) {
	t.Parallel()

	const hash = "56445241b9a3fe7e60745db5181df4b3"
	for name, s := range map[string]string{
		"empty":             "",
		"no colon":          "host" + hash,
		"no type":           ":" + hash,
		"uppercase type":    "Host:" + hash,
		"type with space":   "ho st:" + hash,
		"second colon":      "host:" + hash + ":x",
		"short hash":        "host:" + hash[:30],
		"long hash":         "host:" + hash + "00",
		"uppercase hex":     "host:" + strings.ToUpper(hash),
		"non-hex":           "host:" + strings.Repeat("g", 32),
		"empty hash":        "host:",
		"surrounding space": " host:" + hash,
	} {
		if _, err := identity.ParseFingerprint(s); !errors.Is(err, identity.ErrFingerprint) {
			t.Errorf("%s: ParseFingerprint(%q) err = %v, want ErrFingerprint", name, s, err)
		}
	}
}

func TestFingerprintForms(t *testing.T) {
	t.Parallel()

	id, err := newResolver().Resolve(catalog.Host, attrs(catalog.HostID, "h1"))
	if err != nil {
		t.Fatal(err)
	}
	fp := id.Fingerprint()

	if fp.Type() != catalog.Host || fp.IsZero() {
		t.Errorf("Type() = %s, IsZero() = %v", fp.Type(), fp.IsZero())
	}
	if got, want := fp.String(), "host:56445241b9a3fe7e60745db5181df4b3"; got != want {
		t.Errorf("String() = %s, want %s", got, want)
	}
	if id.String() != fp.String() {
		t.Errorf("Identity.String() = %s, want the fingerprint", id.String())
	}

	// Usable as a map key and as JSON text, as a value and as a map key.
	m := map[identity.Fingerprint]int{fp: 1}
	if m[fp] != 1 {
		t.Error("a fingerprint did not work as a map key")
	}
	raw, err := json.Marshal(map[string]any{"handle": fp, "byHandle": m})
	if err != nil {
		t.Fatal(err)
	}
	var back struct {
		Handle   identity.Fingerprint
		ByHandle map[identity.Fingerprint]int
	}
	if err := json.Unmarshal([]byte(strings.NewReplacer("handle", "Handle", "byHandle", "ByHandle").Replace(string(raw))), &back); err != nil {
		t.Fatalf("%v in %s", err, raw)
	}
	if back.Handle != fp || back.ByHandle[fp] != 1 {
		t.Errorf("JSON round trip lost the fingerprint: %+v from %s", back, raw)
	}
}

func TestFingerprintHash(t *testing.T) {
	t.Parallel()

	id, err := newResolver().Resolve(catalog.Host, attrs(catalog.HostID, "h1"))
	if err != nil {
		t.Fatal(err)
	}
	h := id.Fingerprint().Hash()
	if got, want := hex.EncodeToString(h[:]), "56445241b9a3fe7e60745db5181df4b3"; got != want {
		t.Errorf("Hash() = %s, want %s", got, want)
	}
	if got := id.Fingerprint().String(); got != "host:"+hex.EncodeToString(h[:]) {
		t.Errorf("String() = %s does not end with the hash", got)
	}
	var zero identity.Fingerprint
	if zero.Hash() != ([identity.FingerprintBytes]byte{}) {
		t.Error("the zero fingerprint has a non-zero hash")
	}
}

func TestZeroValues(t *testing.T) {
	t.Parallel()

	var fp identity.Fingerprint
	if !fp.IsZero() || fp.String() != "" {
		t.Errorf("zero fingerprint: IsZero() = %v, String() = %q", fp.IsZero(), fp.String())
	}
	if _, err := fp.MarshalText(); !errors.Is(err, identity.ErrFingerprint) {
		t.Errorf("MarshalText of the zero fingerprint: err = %v, want ErrFingerprint", err)
	}

	var id identity.Identity
	if !id.IsZero() || id.Canonical() != "" || id.Attrs() != nil {
		t.Errorf("zero identity: %+v", id)
	}
}

func TestFingerprintFromHashRejectsAnInvalidType(t *testing.T) {
	t.Parallel()

	var hash [identity.FingerprintBytes]byte
	for name, typ := range map[string]catalog.EntityType{
		"empty":     "",
		"uppercase": "Host",
		"space":     "ho st",
		"colon":     "host:x",
	} {
		fp, err := identity.FingerprintFromHash(typ, hash)
		if !errors.Is(err, identity.ErrFingerprint) || !fp.IsZero() {
			t.Errorf("%s: FingerprintFromHash(%q) = %v, %v; want the zero fingerprint and ErrFingerprint", name, typ, fp, err)
		}
	}
}

// A digest has no invalid value, so even the all-zero hash makes a fingerprint:
// the zero fingerprint is the empty type, not a zero hash.
func TestFingerprintFromHashAcceptsAnyHash(t *testing.T) {
	t.Parallel()

	var hash [identity.FingerprintBytes]byte
	fp, err := identity.FingerprintFromHash(catalog.Host, hash)
	if err != nil || fp.IsZero() {
		t.Fatalf("FingerprintFromHash(host, zero hash) = %v, %v", fp, err)
	}
	if got, want := fp.String(), "host:"+strings.Repeat("0", 2*identity.FingerprintBytes); got != want {
		t.Errorf("String() = %s, want %s", got, want)
	}
	back, err := identity.ParseFingerprint(fp.String())
	if err != nil || back != fp {
		t.Errorf("ParseFingerprint(String()) = %v, %v; want %s", back, err, fp)
	}
}
