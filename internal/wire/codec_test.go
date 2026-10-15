package wire_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/wire"
)

var update = flag.Bool("update", false, "write testdata/golden from the fixtures. Only for a new fixture, or when the format's version is raised: a golden file is never rewritten to make a test pass")

const goldenDir = "testdata/golden"

// TestMinimalBytes is the hand-checked example: the bytes of the smallest
// document, written out from the layout in the package documentation, apart from
// the codec. Each line is one field.
func TestMinimalBytes(t *testing.T) {
	var want []byte
	add := func(bs ...byte) { want = append(want, bs...) }
	zeros := func(n int) []byte { return make([]byte, n) }

	add('T', 'S', 'G', 'F', 1, 0) // magic, major 1, minor 0

	// Section 1, the header: 93 bytes.
	add(0x01, 93)
	add(0x02)                                           // basis: derived
	add(0, 0, 0, 0, 0, 0, 0, 7)                         // token 7
	add(0, 0, 0, 0, 0, 0, 0x03, 0xe8)                   // From: 1000 ns
	add(0, 0, 0, 0, 0, 0, 0x04, 0x4c)                   // To: 1100 ns, a window of 100
	add(2, 1, 1, 1, 1, 1, 1)                            // policy: subgraph 2, interval, coresidency, extract, blastradius, graphdiff, identity encoding 1
	add(zeros(32)...)                                   // catalog id digest, zero in this fixture
	add(0)                                              // rank digest: none
	add(0x01)                                           // query kind: scope
	add(1)                                              // one seed
	add(0x04)                                           // the seed's entity id: host is 4
	add(0, 1)                                           // the first two bytes of the hash,
	add(zeros(14)...)                                   // and the other 14
	add(0)                                              // no options
	add(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff) // asof: store.Latest

	// Section 2, the dictionary: 21 bytes.
	add(0x02, 21)
	add(1)          // one entity
	add(1, 0x04, 1) // one run: type 4 (host), one entity
	add(0, 1)       // its hash, 16 bytes
	add(zeros(14)...)
	add(0x00) // flags: not a boundary

	// Section 3, the elements: 15 bytes.
	add(0x03, 15)
	add(1, 0, 0) // one entity element, no edge, no derived
	add(0)       // producer table: empty
	add(0)       // entity ids: dictionary id 0
	add(0x01)    // class: stable
	add(0)       // transitions: 0
	add(0x00)    // subject flags: asserted
	add(1)       // pieces: one
	add(0)       // piece start: 0
	add(99)      // piece length less one: 99, so the piece is [0, 100)
	add(0x00)    // before flags: not before From
	add(0)       // end reason: unknown, the piece reaches To
	add(0)       // producers of the piece: none
	add(0)       // leg ends of the piece: none

	// Section 4, the keyframes: 3 bytes.
	add(0x04, 3)
	add(1) // one keyframe, at From
	add(1) // with one element
	add(0) // element 0

	add(0x05, 1, 0)             // Section 5: no deltas
	add(0x06, 5, 0, 0, 0, 0, 0) // Section 6: no quarantined entity, and a static block of three zero counts and an empty producer table
	add(0x07, 12)               // Section 7: truncated: six empty lists, no frontier, no per-via, four zero counts
	add(zeros(12)...)
	add(0x08, 1, 0)       // Section 8: no unattributed
	add(0x09, 1, 0)       // Section 9: no unmapped
	add(0x0a, 1, 0)       // Section 10: no suppressed
	add(0x0b, 1, 0)       // Section 11: no pod section
	add(0x0c, 3, 0, 0, 0) // Section 12: anchor none, ubiquity not judged, zero frontier nodes

	got := mustEncode(t, minimalDoc())
	if !bytes.Equal(got, want) {
		t.Fatalf("Encode(minimal):\n got % x\nwant % x", got, want)
	}
	back, err := wire.Decode(want)
	if err != nil {
		t.Fatalf("Decode of the hand-written bytes: %v", err)
	}
	if !equal(back, minimalDoc()) {
		t.Fatalf("Decode of the hand-written bytes is not the document:\n%+v\n%+v", back, minimalDoc())
	}
}

func readGolden(t *testing.T, name string) (bin, js []byte) {
	t.Helper()
	bin, err := os.ReadFile(filepath.Join(goldenDir, name+".bin"))
	if err != nil {
		t.Fatalf("golden file: %v (a new fixture is written with -update)", err)
	}
	js, err = os.ReadFile(filepath.Join(goldenDir, name+".json"))
	if err != nil {
		t.Fatalf("golden twin: %v", err)
	}
	return bin, js
}

// TestUpdateGolden writes the golden files that are missing, only with -update,
// and never overwrites one: a golden file is the format's record. A change to one
// is a new fixture under a new name, or, with a version change of the format that
// the PR states, a file deleted by hand.
func TestUpdateGolden(t *testing.T) {
	if !*update {
		t.Skip("run with -update to write the golden files that are missing")
	}
	if err := os.MkdirAll(goldenDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures() {
		d := f.doc(t)
		js, err := wire.JSON(d)
		if err != nil {
			t.Fatalf("%s: JSON: %v", f.name, err)
		}
		for ext, b := range map[string][]byte{".bin": mustEncode(t, d), ".json": js} {
			path := filepath.Join(goldenDir, f.name+ext)
			if _, err := os.Stat(path); err == nil {
				t.Logf("%s exists and is left as it is", path)
				continue
			}
			if err := os.WriteFile(path, b, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Every golden file decodes to its twin and encodes back to the same bytes. It
// uses no builder: the files are the format's record.
func TestGolden(t *testing.T) {
	for _, f := range fixtures() {
		t.Run(f.name, func(t *testing.T) {
			bin, twin := readGolden(t, f.name)
			doc, err := wire.Decode(bin)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			js, err := wire.JSON(doc)
			if err != nil {
				t.Fatalf("JSON: %v", err)
			}
			if !bytes.Equal(js, twin) {
				t.Errorf("the golden decodes to a document that is not its JSON twin:\n%s\nwant\n%s", js, twin)
			}
			again, err := wire.Encode(doc)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !bytes.Equal(again, bin) {
				t.Errorf("Encode(Decode(golden)) differs from the golden:\n% x\n% x", again, bin)
			}
			var m map[string]any
			if err := json.Unmarshal(twin, &m); err != nil {
				t.Fatalf("the twin is not JSON: %v", err)
			}
			if _, ok := m["token"].(string); !ok {
				t.Errorf("the token of the twin is %T, not a decimal string", m["token"])
			}
		})
	}
}

// The fixtures, made by the builder from hand-built inputs, are what the golden
// files hold. This is the one test that needs the builder.
func TestFixturesEncodeToTheGoldens(t *testing.T) {
	for _, f := range fixtures() {
		t.Run(f.name, func(t *testing.T) {
			bin, _ := readGolden(t, f.name)
			fx := f.doc(t)
			if got := mustEncode(t, fx); !bytes.Equal(got, bin) {
				t.Errorf("the fixture %q no longer encodes to its golden file. If the builder or the codec changed what it makes, "+
					"the golden file stays as it is: it is the format's record, and is never rewritten to make this pass. "+
					"Add a NEW fixture under a new name with -update, and change this one only with a version change of the format", f.name)
			}
			doc, err := wire.Decode(bin)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !equal(doc, fx) {
				t.Errorf("the golden %q no longer decodes to its fixture (see the message of the encoding check)", f.name)
			}
		})
	}
}

// The golden directory holds a .bin and a .json for each fixture, and nothing
// else.
func TestGoldenFilesPairUp(t *testing.T) {
	entries, err := os.ReadDir(goldenDir)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, e := range entries {
		have[e.Name()] = true
	}
	for _, f := range fixtures() {
		for _, ext := range []string{".bin", ".json"} {
			if !have[f.name+ext] {
				t.Errorf("%s%s is missing", f.name, ext)
			}
			delete(have, f.name+ext)
		}
	}
	for name := range have {
		t.Errorf("%s is in %s and is not a fixture's .bin or .json", name, goldenDir)
	}
}

// The window over 2^53 nanoseconds round-trips, and what it holds really is past
// 2^53: the window, a delta, a piece and its length, a span.
func TestWindowOverTwoToThe53(t *testing.T) {
	bin, _ := readGolden(t, "window_over_2pow53")
	doc, err := wire.Decode(bin)
	if err != nil {
		t.Fatal(err)
	}
	sg := doc.Subgraph
	if w := uint64(sg.To.Sub(sg.From)); w <= pow53 {
		t.Fatalf("the window is %d ns, not over 2^53", w)
	}
	var delta, length, span bool
	for _, d := range sg.Deltas {
		delta = delta || d.At > pow53
	}
	for _, el := range sg.Elements {
		for _, p := range el.Pieces {
			length = length || p.End-p.Start > pow53
		}
	}
	for _, u := range sg.Sections.Unattributed {
		for _, s := range u.Spans {
			span = span || s.Start > pow53
		}
	}
	if !delta || !length || !span {
		t.Errorf("past 2^53: a delta %v, a piece longer than it %v, a span %v", delta, length, span)
	}
	// A float holds 2^53+1 as 2^53: the values that differ by one survive.
	times := map[uint64]bool{}
	for _, d := range sg.Deltas {
		times[d.At] = true
	}
	for _, want := range []uint64{pow53 + 7, pow53 + 1000, pow53 + 12345} {
		if !times[want] {
			t.Errorf("no delta at 2^53+%d", want-pow53)
		}
	}
}

// An unknown optional section is skipped, wherever the tag is above 0x7f, in a
// minor version this decoder does not know.
func TestUnknownOptionalSectionIsSkipped(t *testing.T) {
	for _, f := range fixtures() {
		t.Run(f.name, func(t *testing.T) {
			b, _ := readGolden(t, f.name)
			want, err := wire.Decode(b)
			if err != nil {
				t.Fatal(err)
			}
			ss := sections(t, b)
			extra := append(slices.Clone(ss), rawSection{0x81, []byte("a section of a later minor version")}, rawSection{0xff, nil})
			got, err := wire.Decode(assemble(1, 7, extra...))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !equal(got, want) {
				t.Errorf("skipping the sections changed the document")
			}
			if again := mustEncode(t, got); !bytes.Equal(again, b) {
				t.Errorf("a document that skipped sections does not encode to the canonical bytes")
			}
		})
	}
}

func TestRefusals(t *testing.T) {
	bin, _ := readGolden(t, "scope")
	ss := sections(t, bin)
	deltas := sectionOf(t, ss, 0x05).payload
	n, k := binary.Uvarint(deltas)
	if n < 2 {
		t.Fatalf("the scope fixture has %d deltas", n)
	}
	// The operation column follows the time and element columns.
	opAt := k
	for range 2 * int(n) {
		_, m := binary.Uvarint(deltas[opAt:])
		opAt += m
	}
	setOp := func(op byte) []byte {
		p := slices.Clone(deltas)
		p[opAt] = op
		return assemble(1, 0, withSection(ss, 0x05, p)...)
	}
	if got := deltas[opAt]; got != 1 && got != 2 {
		t.Fatalf("the byte at the operation column is %d, not an operation", got)
	}
	trailing := append(slices.Clone(sectionOf(t, ss, 0x08).payload), 0)

	for _, c := range []struct {
		name string
		b    []byte
		want error
	}{
		{"a new major version", assemble(2, 0, ss...), wire.ErrVersion},
		{"major version 0", assemble(0, 0, ss...), wire.ErrVersion},
		{"the reserved attr operation", setOp(3), wire.ErrReserved},
		{"an operation that is not one", setOp(4), wire.ErrFormat},
		{"the reserved rebase section", assemble(1, 0, append(slices.Clone(ss), rawSection{0x20, nil})...), wire.ErrReserved},
		{"an unknown required section", assemble(1, 0, append(slices.Clone(ss), rawSection{0x30, nil})...), wire.ErrFormat},
		{"a required section missing", assemble(1, 0, ss[:len(ss)-1]...), wire.ErrFormat},
		{"a required section twice", assemble(1, 0, append(slices.Clone(ss), ss[len(ss)-1])...), wire.ErrFormat},
		{"sections out of order", assemble(1, 0, slices.Concat(ss[1:2], ss[:1], ss[2:])...), wire.ErrFormat},
		{"an optional section before the end of the required ones", assemble(1, 0, slices.Concat(ss[:3], []rawSection{{0x90, nil}}, ss[3:])...), wire.ErrFormat},
		{"two optional sections with one tag", assemble(1, 0, append(slices.Clone(ss), rawSection{0x90, nil}, rawSection{0x90, nil})...), wire.ErrFormat},
		{"bytes after the end of a section", assemble(1, 0, withSection(ss, 0x08, trailing)...), wire.ErrFormat},
		{"a varint that is not the shortest", assemble(1, 0, withSection(ss, 0x08, []byte{0x80, 0x00})...), wire.ErrFormat},
		{"a varint of eleven bytes", assemble(1, 0, withSection(ss, 0x08, bytes.Repeat([]byte{0x80}, 11))...), wire.ErrFormat},
		{"no magic", append([]byte("XSGF"), bin[4:]...), wire.ErrFormat},
		{"nothing", nil, wire.ErrFormat},
		{"a section longer than the document", append(slices.Clone(bin), 0x90, 0x05, 1), wire.ErrFormat},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := wire.Decode(c.b)
			if !errors.Is(err, c.want) {
				t.Errorf("Decode: %v, want %v", err, c.want)
			}
		})
	}
}

func sectionOf(t *testing.T, ss []rawSection, tag byte) rawSection {
	t.Helper()
	for _, s := range ss {
		if s.tag == tag {
			return s
		}
	}
	t.Fatalf("no section %#x", tag)
	return rawSection{}
}

// A count the bytes cannot hold is refused before anything is allocated for it,
// in every section and column that takes one.
func TestHostileCountsAllocateNothing(t *testing.T) {
	huge := binary.AppendUvarint(nil, 1<<60)
	mid := binary.AppendUvarint(nil, 1<<20)
	zero := []byte{0}
	for _, name := range []string{"scope", "extraction", "quarantine"} {
		bin, _ := readGolden(t, name)
		ss := sections(t, bin)
		for _, s := range ss {
			// Replace the section with a count, then whatever the real payload
			// holds after its first number: the count is wrong for the rest.
			_, k := binary.Uvarint(s.payload)
			for _, count := range [][]byte{huge, mid} {
				tampered := slices.Concat(count, s.payload[k:])
				for _, p := range [][]byte{tampered, slices.Concat(zero, zero, count, s.payload[k:]), slices.Concat(zero, count)} {
					b := assemble(1, 0, withSection(ss, s.tag, p)...)
					type result struct {
						err  error
						used uint64
					}
					done := make(chan result, 1)
					go func() {
						var before, after runtime.MemStats
						runtime.GC()
						runtime.ReadMemStats(&before)
						_, err := wire.Decode(b)
						runtime.ReadMemStats(&after)
						done <- result{err, after.TotalAlloc - before.TotalAlloc}
					}()
					var res result
					select {
					case res = <-done:
					case <-time.After(2 * time.Second):
						t.Fatalf("%s section %#x with a count of %x: Decode of %d bytes was still running after 2 s, "+
							"so a count is not checked against the bytes that remain", name, s.tag, count, len(b))
					}
					if res.err == nil {
						continue
					}
					if !errors.Is(res.err, wire.ErrFormat) && !errors.Is(res.err, wire.ErrReserved) {
						t.Errorf("%s section %#x: error %v", name, s.tag, res.err)
					}
					if res.used > 1<<20 {
						t.Errorf("%s section %#x with a count of %x: %d bytes allocated for %d bytes of input", name, s.tag, count, res.used, len(b))
					}
				}
			}
		}
	}
}

// A document cut anywhere, and a document with any byte changed, is either
// refused or is another document's canonical encoding: nothing panics, and
// nothing is accepted that Encode would not write.
func TestDecodeOfDamagedBytes(t *testing.T) {
	for _, name := range []string{"minimal", "scope", "quarantine"} {
		bin, _ := readGolden(t, name)
		for i := range bin {
			if _, err := wire.Decode(bin[:i]); err == nil {
				t.Errorf("%s cut to %d of %d bytes decodes", name, i, len(bin))
			}
		}
		if name != "minimal" {
			continue
		}
		for i := range bin {
			if i == 5 {
				continue // the minor version: a later one is read, and written back as this one
			}
			for _, x := range []byte{1, 2, 0x80, 0xff, 0x55} {
				b := slices.Clone(bin)
				b[i] ^= x
				doc, err := wire.Decode(b)
				if err != nil {
					continue
				}
				if again, err := wire.Encode(doc); err != nil || !bytes.Equal(again, b) {
					t.Errorf("byte %d xor %#x: decodes, and does not encode back (%v)", i, x, err)
				}
			}
		}
	}
}

// What Decode refuses, Encode refuses, and both refuse an out-of-range enum
// value: a wire value is never a cast of a Go constant that happens to pass.
func TestEncodeRefusesWhatDecodeRefuses(t *testing.T) {
	type mutation struct {
		name    string
		fixture string
		apply   func(*wire.Document)
	}
	elem := func(d *wire.Document, i int) *subgraph.Element { return &d.Subgraph.Elements[i] }
	pod1 := func(d *wire.Document) *subgraph.Element {
		i, ok := d.Subgraph.ElementID(ent(pod(1)))
		if !ok {
			t.Fatal("no element for pod 1")
		}
		return elem(d, int(i))
	}
	for _, m := range []mutation{
		{"basis fact", "scope", func(d *wire.Document) { d.Subgraph.Basis = subgraph.BasisFact }},
		{"basis outside the table", "scope", func(d *wire.Document) { d.Subgraph.Basis = 9 }},
		{"query kind outside the table", "scope", func(d *wire.Document) { d.Query.Kind = 9 }},
		{"query kind zero", "scope", func(d *wire.Document) { d.Query.Kind = 0 }},
		{"options out of order", "coresidents", func(d *wire.Document) { slices.Reverse(d.Query.Options) }},
		{"a repeated option", "coresidents", func(d *wire.Document) { d.Query.Options[1].Name = d.Query.Options[0].Name }},
		{"an option without a name", "coresidents", func(d *wire.Document) { d.Query.Options[0].Name = "" }},
		{"a rank digest of the wrong size", "scope", func(d *wire.Document) { d.Policy.Rank = []byte{1, 2, 3} }},
		{"a negative policy version", "scope", func(d *wire.Document) { d.Policy.Interval = -1 }},
		{"an empty window", "scope", func(d *wire.Document) { d.Subgraph.To = d.Subgraph.From }},
		{"a window before the epoch", "scope", func(d *wire.Document) { d.Subgraph.From = store.MinEventTime.Add(-1) }},
		{"a dictionary out of order", "scope", func(d *wire.Document) { slices.Reverse(d.Subgraph.Entities) }},
		{"an element out of order", "scope", func(d *wire.Document) {
			slices.Reverse(d.Subgraph.Elements)
		}},
		{"a class that its pieces do not make", "scope", func(d *wire.Document) { pod1(d).Class = subgraph.ClassTransient }},
		{"a class outside the table", "scope", func(d *wire.Document) { pod1(d).Class = 9 }},
		{"transitions that its pieces do not make", "scope", func(d *wire.Document) { pod1(d).Transitions++ }},
		{"a subject its key does not make", "coresidents", func(d *wire.Document) {
			last := len(d.Subgraph.Elements) - 1
			elem(d, last).Subject = subgraph.SubjectAsserted
		}},
		{"a rule its key does not make", "coresidents", func(d *wire.Document) {
			last := len(d.Subgraph.Elements) - 1
			elem(d, last).Rule = subgraph.RuleExistence
		}},
		{"a derived key with a rule outside the table", "coresidents", func(d *wire.Document) {
			last := len(d.Subgraph.Elements) - 1
			elem(d, last).Key.Rule = "pair.nothing"
		}},
		{"a piece that is empty", "scope", func(d *wire.Document) { pod1(d).Pieces[0].End = pod1(d).Pieces[0].Start }},
		{"pieces that touch", "scope", func(d *wire.Document) { pod1(d).Pieces[1].Start = pod1(d).Pieces[0].End }},
		{"a piece past the window", "scope", func(d *wire.Document) {
			p := pod1(d).Pieces
			p[len(p)-1].End += 1
		}},
		{"an end reason outside the table", "scope", func(d *wire.Document) { pod1(d).Pieces[0].EndReason = 9 }},
		{"unknown as an end inside the window", "scope", func(d *wire.Document) { pod1(d).Pieces[0].EndReason = subgraph.EndReasonUnknown }},
		{"not_seen_in_window as an end", "scope", func(d *wire.Document) { pod1(d).Pieces[0].EndReason = subgraph.EndReasonNotSeenInWindow }},
		{"a reason only a derived span has, on an asserted subject", "scope", func(d *wire.Document) {
			pod1(d).Pieces[0].EndReason = subgraph.EndReasonReachEnded
		}},
		{"producers out of order", "scope", func(d *wire.Document) { slices.Reverse(pod1(d).Pieces[0].Producers) }},
		{"before on a piece that starts after From", "scope", func(d *wire.Document) { pod1(d).Pieces[1].Before = true }},
		{"a leg that is not an element", "coresidents", func(d *wire.Document) {
			last := len(d.Subgraph.Elements) - 1
			elem(d, last).Paths[0][0] = 1000
		}},
		{"a leg that is not an edge", "coresidents", func(d *wire.Document) {
			last := len(d.Subgraph.Elements) - 1
			elem(d, last).Paths[0][0] = 0
		}},
		{"a leg end for a leg that is not there", "coresidents", func(d *wire.Document) {
			last := len(d.Subgraph.Elements) - 1
			elem(d, last).Pieces[0].Ends[0].Leg = 9
		}},
		{"a keyframe that does not start at From", "scope", func(d *wire.Document) { d.Subgraph.Keyframes[0].At = 1 }},
		{"no keyframe", "scope", func(d *wire.Document) { d.Subgraph.Keyframes = nil }},
		{"a keyframe at To", "scope", func(d *wire.Document) {
			d.Subgraph.Keyframes = append(d.Subgraph.Keyframes, subgraph.Keyframe{At: uint64(d.Subgraph.To.Sub(d.Subgraph.From))})
		}},
		{"keyframes out of order", "scope", func(d *wire.Document) { slices.Reverse(d.Subgraph.Keyframes) }},
		{"a keyframe element that does not exist", "scope", func(d *wire.Document) { d.Subgraph.Keyframes[0].Elements = []uint32{9999} }},
		{"keyframe elements out of order", "scope", func(d *wire.Document) { slices.Reverse(d.Subgraph.Keyframes[0].Elements) }},
		{"deltas that decrease in time", "scope", func(d *wire.Document) {
			ds := d.Subgraph.Deltas
			ds[0].At, ds[len(ds)-1].At = ds[len(ds)-1].At, ds[0].At
		}},
		{"deltas of one instant out of element order", "scope", func(d *wire.Document) {
			ds := d.Subgraph.Deltas
			for i := 1; i < len(ds); i++ {
				if ds[i].At == ds[i-1].At {
					ds[i], ds[i-1] = ds[i-1], ds[i]
					return
				}
			}
			t.Fatal("no two deltas at one instant")
		}},
		{"a delta at From", "scope", func(d *wire.Document) { d.Subgraph.Deltas[0].At = 0 }},
		{"a delta at To", "scope", func(d *wire.Document) {
			d.Subgraph.Deltas[len(d.Subgraph.Deltas)-1].At = uint64(d.Subgraph.To.Sub(d.Subgraph.From))
		}},
		{"a delta of an element that does not exist", "scope", func(d *wire.Document) { d.Subgraph.Deltas[0].Element = 9999 }},
		{"a disappearance whose reason its piece does not have", "scope", func(d *wire.Document) {
			for i, dl := range d.Subgraph.Deltas {
				if !dl.Appear() {
					r := subgraph.EndReasonProducer
					if dl.After.Reason == r {
						r = subgraph.EndReasonCascade
					}
					d.Subgraph.Deltas[i].After.Reason = r
					return
				}
			}
			t.Fatal("no disappearance")
		}},
		{"an appearance whose before-state is not the end of the piece before", "scope", func(d *wire.Document) {
			for i, dl := range d.Subgraph.Deltas {
				if dl.Appear() {
					r := subgraph.EndReasonNotSeenInWindow
					if dl.Before.Reason == r {
						r = subgraph.EndReasonProducer
					}
					d.Subgraph.Deltas[i].Before.Reason = r
					return
				}
			}
			t.Fatal("no appearance")
		}},
		{"a piece boundary with no delta", "scope", func(d *wire.Document) {
			d.Subgraph.Deltas = d.Subgraph.Deltas[:len(d.Subgraph.Deltas)-1]
		}},
		{"a keyframe that lacks an element present then", "scope", func(d *wire.Document) {
			k := &d.Subgraph.Keyframes[0]
			k.Elements = k.Elements[:len(k.Elements)-1]
		}},
		{"a keyframe that lists an element not present then", "scope", func(d *wire.Document) {
			k := &d.Subgraph.Keyframes[len(d.Subgraph.Keyframes)-1]
			k.Elements = nil
			for i := range d.Subgraph.Elements {
				k.Elements = append(k.Elements, uint32(i))
			}
		}},
		{"a keyframe of the right size with an element that is not present then", "scope", func(d *wire.Document) {
			k := &d.Subgraph.Keyframes[len(d.Subgraph.Keyframes)-1]
			in := map[uint32]bool{}
			for _, id := range k.Elements {
				in[id] = true
			}
			for x := range d.Subgraph.Elements {
				if !in[uint32(x)] && len(k.Elements) > 0 {
					swapped := append([]uint32{uint32(x)}, k.Elements[1:]...)
					slices.Sort(swapped)
					k.Elements = swapped
					return
				}
			}
			t.Fatal("every element is in the last keyframe")
		}},
		{"a keyframe that lacks an element of a later instant", "scope", func(d *wire.Document) {
			k := &d.Subgraph.Keyframes[len(d.Subgraph.Keyframes)-1]
			k.Elements = k.Elements[1:]
		}},
		{"a delta that is neither", "scope", func(d *wire.Document) { d.Subgraph.Deltas[0].After.Present = d.Subgraph.Deltas[0].Before.Present }},
		{"a disappearance with unknown", "scope", func(d *wire.Document) {
			for i, dl := range d.Subgraph.Deltas {
				if !dl.Appear() {
					d.Subgraph.Deltas[i].After.Reason = subgraph.EndReasonUnknown
					return
				}
			}
		}},
		{"a quarantined entity without its collision record", "quarantine", func(d *wire.Document) { d.Subgraph.Quarantine.Entities[0].Cause = nil }},
		{"a quarantine layer outside the table", "quarantine", func(d *wire.Document) { d.Subgraph.Quarantine.Entities[0].Cause.Layer = 0 }},
		{"a static edge whose cause is not its endpoint", "quarantine", func(d *wire.Document) {
			d.Subgraph.Quarantine.Edges[0].Cause.Entity = d.Subgraph.Entities[0].Fingerprint
		}},
		{"a section entity that is not in the dictionary", "coresidents", func(d *wire.Document) {
			d.Subgraph.Sections.Truncated.Nodes = append(d.Subgraph.Sections.Truncated.Nodes, mkfp(catalog.Host, 9999))
		}},
		{"a mechanism outside the table", "coresidents", func(d *wire.Document) { d.Subgraph.Sections.Truncated.PerVia[0].Mechanism = 9 }},
		{"a per-via count of zero", "coresidents", func(d *wire.Document) { d.Subgraph.Sections.Truncated.PerVia[0].Dropped = 0 }},
		{"a negative count of what was cut", "coresidents", func(d *wire.Document) { d.Subgraph.Sections.Truncated.Results = -1 }},
		{"an unattributed reason outside the table", "coresidents", func(d *wire.Document) { d.Subgraph.Sections.Unattributed[0].Reason = 9 }},
		{"a relation on an unattributed entry that does not claim", "coresidents", func(d *wire.Document) { d.Subgraph.Sections.Unattributed[0].Relation = catalog.RunsOn }},
		{"a span with the wrong end reason", "coresidents", func(d *wire.Document) {
			d.Subgraph.Sections.Unattributed[0].Spans[0].EndReason = subgraph.EndReasonUnknown
		}},
		{"spans that touch", "coresidents", func(d *wire.Document) {
			s := d.Subgraph.Sections.Unattributed[0].Spans
			s[1].Start = s[0].End
		}},
		{"a suppress reason outside the table", "coresidents", func(d *wire.Document) { d.Subgraph.Sections.Suppressed[0].Reason = 9 }},
		{"a coverage on an entry that is not ubiquitous", "coresidents", func(d *wire.Document) { d.Subgraph.Sections.Suppressed[0].Coverage = [2]int{1, 2} }},
		{"a suppressed entry with the wrong class", "coresidents", func(d *wire.Document) { d.Subgraph.Sections.Suppressed[0].Class = subgraph.ClassStable }},
		{"an anchor state outside the table", "extraction", func(d *wire.Document) { d.Subgraph.Sections.Anchor = 9 }},
		{"a pod section that lists a key the quarantine lacks", "extraction", func(d *wire.Document) {
			d.Subgraph.Sections.Pods[0].Quarantined = append(d.Subgraph.Sections.Pods[0].Quarantined, ent(pod(1)))
		}},
		{"two pod sections of one seed", "extraction", func(d *wire.Document) {
			d.Subgraph.Sections.Pods[1].Seed = d.Subgraph.Sections.Pods[0].Seed
		}},
	} {
		t.Run(m.name, func(t *testing.T) {
			bin, _ := readGolden(t, m.fixture)
			d, err := wire.Decode(bin)
			if err != nil {
				t.Fatal(err)
			}
			m.apply(&d)
			if _, err := wire.Encode(d); !errors.Is(err, store.ErrInvalid) {
				t.Errorf("Encode: %v, want an error wrapping store.ErrInvalid", err)
			}
			if _, err := wire.JSON(d); !errors.Is(err, store.ErrInvalid) {
				t.Errorf("JSON: %v, want an error wrapping store.ErrInvalid", err)
			}
		})
	}
}

// A producer in the table that no piece uses is another encoding of the same
// document, so it is refused.
func TestUnusedProducerIsRefused(t *testing.T) {
	bin, _ := readGolden(t, "minimal")
	ss := sections(t, bin)
	p := sectionOf(t, ss, 0x03).payload
	// The three counts, then the table, which is empty.
	if !bytes.Equal(p[:4], []byte{1, 0, 0, 0}) {
		t.Fatalf("the elements of the minimal document start % x", p[:4])
	}
	tampered := slices.Concat(p[:3], []byte{1, 1, 'x'}, p[4:])
	if _, err := wire.Decode(assemble(1, 0, withSection(ss, 0x03, tampered)...)); !errors.Is(err, wire.ErrFormat) {
		t.Errorf("Decode: %v, want %v", err, wire.ErrFormat)
	}
}

// The wire values of the enums the format owns are frozen.
func TestQueryKindTable(t *testing.T) {
	for _, c := range []struct {
		k    wire.QueryKind
		v    uint8
		name string
	}{
		{wire.QueryScope, 1, "scope"},
		{wire.QueryCoResidents, 2, "co_residents"},
		{wire.QueryExtraction, 3, "extraction"},
		{wire.QueryBlastRadius, 4, "blast_radius"},
	} {
		if uint8(c.k) != c.v || c.k.String() != c.name || !c.k.Valid() {
			t.Errorf("%v: value %d, name %q, valid %v; want %d, %q", c.k, uint8(c.k), c.k.String(), c.k.Valid(), c.v, c.name)
		}
	}
	for _, k := range []wire.QueryKind{0, 5, 255} {
		if k.Valid() || !strings.HasPrefix(k.String(), "QueryKind(") {
			t.Errorf("QueryKind(%d) is valid or is named %q", uint8(k), k.String())
		}
	}
}

// The policy vector of this build, and the digest of the catalog's id set, which
// is recomputed here from the frozen ids file.
func TestCurrentPolicy(t *testing.T) {
	p := wire.CurrentPolicy()
	if p.Interval != 1 || p.CoResidency != 1 || p.Extract != 1 || p.BlastRadius != 1 || p.GraphDiff != 1 || p.IdentityEncoding != 1 || p.Rank != nil {
		t.Errorf("the policy vector is %+v", p)
	}
	raw, err := os.ReadFile("../catalog/testdata/ids_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var ids struct {
		Entities  map[string]uint64 `json:"entities"`
		Relations map[string]uint64 `json:"relations"`
	}
	if err := json.Unmarshal(raw, &ids); err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	for _, part := range []struct {
		tag byte
		m   map[string]uint64
	}{{'e', ids.Entities}, {'r', ids.Relations}} {
		type row struct {
			id   uint64
			name string
		}
		var rows []row
		for name, id := range part.m {
			rows = append(rows, row{id, name})
		}
		slices.SortFunc(rows, func(a, b row) int { return int(a.id) - int(b.id) })
		for _, r := range rows {
			h.Write([]byte{part.tag})
			h.Write(binary.AppendUvarint(nil, r.id))
			h.Write(binary.AppendUvarint(nil, uint64(len(r.name))))
			h.Write([]byte(r.name))
		}
	}
	if got, want := hex.EncodeToString(p.CatalogIDs[:]), hex.EncodeToString(h.Sum(nil)); got != want {
		t.Errorf("the catalog digest is %s, the frozen ids file makes %s", got, want)
	}
	const pinned = "f28d37014cb38aaad2b7bfa3bb73194e0db9c69e61ac95942517e82f964dfe70"
	if got := hex.EncodeToString(p.CatalogIDs[:]); got != pinned {
		t.Errorf("the catalog digest is %s, pinned %s", got, pinned)
	}
	if wire.CatalogDigest(catalog.Default()) != p.CatalogIDs {
		t.Error("CurrentPolicy does not carry CatalogDigest of the default catalog")
	}
}
