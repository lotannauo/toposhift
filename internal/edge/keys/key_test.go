package keys

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"testing"
)

// The golden keys were computed outside this package, from the definition in
// the package documentation, for the byte strings named.
func TestKeyFormatGoldenVectors(t *testing.T) {
	t.Parallel()
	seq := make([]byte, 32)
	for i := range seq {
		seq[i] = byte(i)
	}
	ff := bytes.Repeat([]byte{0xff}, 32)
	for _, tc := range []struct {
		name  string
		raw   []byte
		scope Scope
		want  string
	}{
		{"zero read", make([]byte, 32), ScopeRead, "tps_r_" + strings.Repeat("0", 43) + "05dNes"},
		{"zero ingest", make([]byte, 32), ScopeIngest, "tps_i_" + strings.Repeat("0", 43) + "0YABwU"},
		{"sequence read", seq, ScopeRead, "tps_r_003aUlTJC7tjlCTQj2uNU3MFagCXG9LRKRcwGkBIDlf0lffli"},
		{"sequence ingest", seq, ScopeIngest, "tps_i_003aUlTJC7tjlCTQj2uNU3MFagCXG9LRKRcwGkBIDlf0w2AnG"},
		{"all ones read", ff, ScopeRead, "tps_r_yhjskwdA6OZ1AL1YmHWZWm8LLG7HjnuCA2j5rOw8Xp13DEd0E"},
		{"all ones ingest", ff, ScopeIngest, "tps_i_yhjskwdA6OZ1AL1YmHWZWm8LLG7HjnuCA2j5rOw8Xp13KBOOU"},
	} {
		got, err := newSecret(tc.scope, bytes.NewReader(tc.raw))
		if err != nil {
			t.Fatal(err)
		}
		want := tc.want
		if len(want) != textLen {
			t.Fatalf("%s: golden of length %d, want %d", tc.name, len(want), textLen)
		}
		if got.Reveal() != want {
			t.Errorf("%s: %s, want %s", tc.name, got.Reveal(), want)
		}
		if sc, err := Parse(want); err != nil || sc != tc.scope {
			t.Errorf("%s: Parse = %v, %v", tc.name, sc, err)
		}
	}
}

func TestAKeyHolds256BitsOfTheBytesItWasMadeFrom(t *testing.T) {
	t.Parallel()
	// 43 base62 digits hold 256 bits, and 42 would not.
	b := big.NewInt(62)
	if new(big.Int).Exp(b, big.NewInt(bodyLen), nil).Cmp(new(big.Int).Lsh(big.NewInt(1), 256)) <= 0 {
		t.Fatal("62^43 does not exceed 2^256")
	}
	if new(big.Int).Exp(b, big.NewInt(bodyLen-1), nil).Cmp(new(big.Int).Lsh(big.NewInt(1), 256)) >= 0 {
		t.Fatal("42 digits would hold 256 bits")
	}
	src := newRand(1)
	for range 50 {
		raw := make([]byte, 32)
		_, _ = src.Read(raw)
		s, err := newSecret(ScopeRead, bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		// Decode the body back to bytes with an independent reading of the alphabet.
		n := new(big.Int)
		for _, c := range s.Reveal()[6 : 6+bodyLen] {
			n.Mul(n, big.NewInt(62))
			n.Add(n, big.NewInt(int64(strings.IndexRune(alphabet, c))))
		}
		got := n.FillBytes(make([]byte, 32))
		if !bytes.Equal(got, raw) {
			t.Fatalf("body decodes to %x, want %x", got, raw)
		}
	}
}

func TestParseRefusesEveryMistypedCharacter(t *testing.T) {
	t.Parallel()
	for _, scope := range []Scope{ScopeRead, ScopeIngest} {
		st := newTestStore(t, nil)
		issued, err := st.Create(scope, CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		key := issued.Secret.Reveal()
		if got, err := Parse(key); err != nil || got != scope {
			t.Fatalf("Parse of a fresh key = %v, %v", got, err)
		}
		n := 0
		for i := range len(key) {
			for _, c := range []byte(alphabet) {
				if c == key[i] {
					continue
				}
				typo := key[:i] + string(c) + key[i+1:]
				n++
				if _, err := Parse(typo); !errors.Is(err, ErrMalformed) {
					t.Fatalf("position %d changed to %q was not refused: %v", i, c, err)
				}
			}
		}
		// Every position takes the 61 other digits, and the two underscores
		// of the form take all 62.
		if want := textLen*(len(alphabet)-1) + 2; n != want {
			t.Fatalf("%d cases, want %d", n, want)
		}
		// Swapped neighbours that differ, a missing character, an extra one.
		for i := 0; i+1 < len(key); i++ {
			if key[i] == key[i+1] {
				continue
			}
			sw := []byte(key)
			sw[i], sw[i+1] = sw[i+1], sw[i]
			if _, err := Parse(string(sw)); !errors.Is(err, ErrMalformed) {
				t.Fatalf("swap at %d was not refused: %v", i, err)
			}
		}
		for i := range len(key) {
			if _, err := Parse(key[:i] + key[i+1:]); !errors.Is(err, ErrMalformed) {
				t.Fatalf("deleting position %d was not refused", i)
			}
			if _, err := Parse(key[:i] + "A" + key[i:]); !errors.Is(err, ErrMalformed) {
				t.Fatalf("inserting at position %d was not refused", i)
			}
		}
	}
}

func TestParseRefusesWhatIsNotAKey(t *testing.T) {
	t.Parallel()
	st := newTestStore(t, nil)
	issued, err := st.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	k := issued.Secret.Reveal()
	for name, s := range map[string]string{
		"empty":            "",
		"prefix only":      "tps_",
		"upper prefix":     "TPS_" + k[4:],
		"other prefix":     "tpx_" + k[4:],
		"unknown scope":    k[:4] + "x" + k[5:],
		"upper scope":      k[:4] + "R" + k[5:],
		"no separator":     k[:5] + "-" + k[6:],
		"trailing space":   k + " ",
		"leading space":    " " + k,
		"trailing newline": k + "\n",
		"non-ASCII":        k[:10] + "é" + k[12:],
		"truncated":        k[:len(k)-1],
		"doubled":          k + k,
		"hex of a digest":  strings.Repeat("ab", 32),
	} {
		if _, err := Parse(s); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
	if len(k) != 55 || !strings.HasPrefix(k, "tps_r_") {
		t.Errorf("key %q has length %d", k, len(k))
	}
}

func TestParseScope(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"read", "ingest"} {
		if got, err := ParseScope(s); err != nil || string(got) != s {
			t.Errorf("ParseScope(%q) = %v, %v", s, got, err)
		}
	}
	for _, s := range []string{"", "Read", "write", "read ", "admin"} {
		if _, err := ParseScope(s); err == nil {
			t.Errorf("ParseScope(%q) succeeded", s)
		}
	}
}

func TestASecretPrintsAsAPlaceholderEverywhere(t *testing.T) {
	t.Parallel()
	st := newTestStore(t, nil)
	issued, err := st.Create(ScopeRead, CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	text := issued.Secret.Reveal()
	body := text[6:49]

	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("issued", "secret", issued.Secret, "issued", issued)
	slog.New(slog.NewTextHandler(&logged, nil)).Info("issued", "secret", issued.Secret)
	asJSON, err := json.Marshal(map[string]any{"secret": issued.Secret, "issued": issued})
	if err != nil {
		t.Fatal(err)
	}
	for name, out := range map[string]string{
		"%v":      fmt.Sprintf("%v", issued.Secret),
		"%s":      issued.Secret.String(),
		"%q":      fmt.Sprintf("%q", issued.Secret),
		"%+v":     fmt.Sprintf("%+v", issued),
		"%#v":     fmt.Sprintf("%#v", issued),
		"%x":      fmt.Sprintf("%x", issued.Secret),
		"Println": fmt.Sprintln(issued.Secret, issued),
		"slog":    logged.String(),
		"json":    string(asJSON),
		"key":     fmt.Sprintf("%+v %#v", issued.Key, issued.Key),
	} {
		if strings.Contains(out, text) || strings.Contains(out, body) || strings.Contains(out, fmt.Sprintf("%x", text)) {
			t.Errorf("%s output holds the key: %s", name, out)
		}
	}
	if !strings.Contains(logged.String(), redacted) {
		t.Errorf("slog output has no placeholder: %s", logged.String())
	}
}

func TestBearerToken(t *testing.T) {
	t.Parallel()
	const k = "tps_r_abc"
	for _, tc := range []struct {
		name string
		h    http.Header
		want string
		ok   bool
	}{
		{"plain", http.Header{"Authorization": {"Bearer " + k}}, k, true},
		{"scheme in lower case", http.Header{"Authorization": {"bearer " + k}}, k, true},
		{"scheme in upper case", http.Header{"Authorization": {"BEARER " + k}}, k, true},
		{"none", http.Header{}, "", false},
		{"empty", http.Header{"Authorization": {""}}, "", false},
		{"no key", http.Header{"Authorization": {"Bearer "}}, "", false},
		{"no space", http.Header{"Authorization": {"Bearer"}}, "", false},
		{"two spaces", http.Header{"Authorization": {"Bearer  " + k}}, "", false},
		{"trailing space", http.Header{"Authorization": {"Bearer " + k + " "}}, "", false},
		{"inner space", http.Header{"Authorization": {"Bearer " + k + " x"}}, "", false},
		{"control character", http.Header{"Authorization": {"Bearer " + k + "\x01"}}, "", false},
		{"non-ASCII", http.Header{"Authorization": {"Bearer " + k + "é"}}, "", false},
		{"basic", http.Header{"Authorization": {"Basic " + k}}, "", false},
		{"no scheme", http.Header{"Authorization": {k}}, "", false},
		{"repeated", http.Header{"Authorization": {"Bearer " + k, "Bearer " + k}}, "", false},
		{"another header", http.Header{"X-Authorization": {"Bearer " + k}}, "", false},
	} {
		got, ok := BearerToken(tc.h)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s: (%q, %v), want (%q, %v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func FuzzParse(f *testing.F) {
	st := newTestStore(f, nil)
	issued, err := st.Create(ScopeRead, CreateOptions{})
	if err != nil {
		f.Fatal(err)
	}
	good := issued.Secret.Reveal()
	for _, s := range []string{good, "", "tps_", good[:54], good + "x", strings.ToUpper(good)} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		scope, err := Parse(s)
		if err != nil {
			return
		}
		// Whatever is accepted has the form of a key, down to its check.
		if len(s) != textLen || !strings.HasPrefix(s, "tps_") || (scope != ScopeRead && scope != ScopeIngest) {
			t.Fatalf("accepted %q as %v", s, scope)
		}
		for _, c := range []byte(s[6:]) {
			if !strings.ContainsRune(alphabet, rune(c)) {
				t.Fatalf("accepted %q with %q in it", s, c)
			}
		}
	})
}
