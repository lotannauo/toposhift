package workload_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

var allProducers = []lifecycle.Producer{
	workload.ProducerFabric, workload.ProducerK8sObjects, workload.ProducerNodeCollector,
	workload.ProducerTraces, workload.ProducerKubelet, workload.ProducerPodHeartbeat,
}

// A producer length lengthens every producer name to exactly that many bytes, in
// entity records and edge records alike, and changes nothing else: the stream with it
// has the same records in the same order, each the one without it but for its
// producer, and each producer has one name that is its own followed by "/" and filler.
func TestAProducerLenPadsEveryProducerAndChangesNothingElse(t *testing.T) {
	t.Parallel()

	small := workload.Small()
	small.Duration = 10 * time.Minute
	cluster := conformance.Configs()[6] // fresh identities, pod heartbeats, a backlog, coalesced runs, a second producer
	for name, cfg := range map[string]workload.Config{"small": small, "cluster-shaped": cluster} {
		plain := generate(t, cfg)
		if len(plain) == 0 {
			t.Fatalf("%s: no records", name)
		}
		for _, n := range []int{workload.MinProducerLen, 15, 18, 64} {
			padded := cfg
			padded.ProducerLen = n
			long := generate(t, padded)
			if len(long) != len(plain) {
				t.Fatalf("%s, %d bytes: %d records without the option and %d with it", name, n, len(plain), len(long))
			}
			names := map[lifecycle.Producer]lifecycle.Producer{}
			seen := map[lifecycle.Producer]bool{}
			var entities, edges int
			for i := range plain {
				a, b := plain[i], long[i]
				if len(b.Producer) != n {
					t.Fatalf("%s, %d bytes: record %d (%v) has a producer of %d bytes: %q", name, n, i, b.Subject.Kind, len(b.Producer), b.Producer)
				}
				if b.Subject.Kind == engine.SubjectEntity {
					entities++
				} else {
					edges++
				}
				// A producer's name is its own, then "/", then filler: the padding comes after
				// the name, so two producers cannot come to share one.
				if b.Producer != a.Producer {
					if rest, ok := strings.CutPrefix(string(b.Producer), string(a.Producer)+"/"); !ok || rest != workload.ProducerFill(len(rest)) || len(rest) != n-len(a.Producer)-1 {
						t.Fatalf("%s, %d bytes: record %d's producer %q is not %q, then \"/\" and the filler", name, n, i, b.Producer, a.Producer)
					}
					// The filler is not one repeated character, which block compression would hide.
					if rest := string(b.Producer)[len(a.Producer)+1:]; len(rest) >= 2 && strings.Trim(rest, rest[:1]) == "" {
						t.Fatalf("%s, %d bytes: producer %q is padded with one repeated character", name, n, b.Producer)
					}
				}
				for _, c := range string(b.Producer) {
					if !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:/-", c) {
						t.Fatalf("%s, %d bytes: producer %q has the character %q", name, n, b.Producer, c)
					}
				}
				if prev, ok := names[a.Producer]; ok && prev != b.Producer {
					t.Fatalf("%s, %d bytes: %q has the names %q and %q", name, n, a.Producer, prev, b.Producer)
				}
				names[a.Producer], seen[a.Producer] = b.Producer, true
				// Everything else, the subject, the kind, the times, Seq, TTL and the payload, is as it was.
				a2 := b
				a2.Producer = a.Producer
				if digest([]engine.Record{a}) != digest([]engine.Record{a2}) {
					t.Fatalf("%s, %d bytes: record %d differs in more than its producer: %+v and %+v", name, n, i, a, b)
				}
			}
			if entities == 0 || edges == 0 {
				t.Errorf("%s, %d bytes: %d entity records and %d edge records", name, n, entities, edges)
			}
			distinct := map[lifecycle.Producer]bool{}
			for _, padded := range names {
				distinct[padded] = true
			}
			if len(distinct) != len(names) {
				t.Errorf("%s, %d bytes: %d producers share %d names: %v", name, n, len(names), len(distinct), names)
			}
			if name == "cluster-shaped" {
				for _, p := range allProducers {
					if !seen[p] {
						t.Errorf("%s: no record of %q, so its padding is not checked", name, p)
					}
				}
			}
		}
	}
}

// ProducerLen is 0, or from the longest producer name to 64 bytes.
func TestAProducerLenOutOfRangeIsRefused(t *testing.T) {
	t.Parallel()

	longest := 0
	for _, p := range allProducers {
		longest = max(longest, len(p))
	}
	if longest != 14 || workload.MinProducerLen != 14 {
		t.Errorf("the longest producer name is %d bytes and the least length %d, both should be 14", longest, workload.MinProducerLen)
	}
	cfg := workload.Tiny()
	for n, ok := range map[int]bool{-1: false, 0: true, 13: false, 14: true, 18: true, 64: true, 65: false} {
		cfg.ProducerLen = n
		if _, err := workload.New(cfg); (err == nil) != ok {
			t.Errorf("ProducerLen %d: %v, accepted %v", n, err, ok)
		}
	}
}

// A producer length of zero is left out of the config's JSON, so the digest of every
// spec made without it is what it was; any other makes another spec, and so another
// plan and stream.
func TestAProducerLenOfZeroLeavesTheSpecAsItWas(t *testing.T) {
	t.Parallel()

	const frozenCI = "8c2f064625127aa6639ba2000bc61dbf7edff8beaa3524cdde88b3d205364da9" // as in the runner's frozen digests
	spec := runner.DefaultSpec(workload.CI())
	if d, err := spec.Digest(); err != nil || d != frozenCI {
		t.Errorf("the digest of the ci spec is %s (%v), frozen at %s", d, err, frozenCI)
	}
	b, err := json.Marshal(spec)
	if err != nil || strings.Contains(string(b), "ProducerLen") {
		t.Errorf("a spec without a producer length says one (%v): %s", err, b)
	}
	w := workload.CI()
	w.ProducerLen = 18
	on := runner.DefaultSpec(w)
	if d, err := on.Digest(); err != nil || d == frozenCI {
		t.Errorf("a spec with a producer length of 18 has the digest %s (%v) of one without", d, err)
	}
	if b, err := json.Marshal(on); err != nil || !strings.Contains(string(b), `"ProducerLen":18`) {
		t.Errorf("a spec with a producer length does not say it (%v): %s", err, b)
	}
	w.ProducerLen = 64
	if d64, _ := runner.DefaultSpec(w).Digest(); d64 == "" {
		t.Error("no digest")
	} else if d18, _ := on.Digest(); d64 == d18 {
		t.Error("producer lengths of 18 and 64 make the same spec")
	}
}

// The filler of a padded name does not repeat in a short period: block compression would
// otherwise hide what a longer name costs. It is deterministic, and in the characters a
// producer name may have.
func TestTheProducerFillDoesNotRepeat(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 1, 2, 3, 36, 57, 62, 64} {
		f := workload.ProducerFill(n)
		if len(f) != n || f != workload.ProducerFill(n) {
			t.Fatalf("fill(%d) = %q", n, f)
		}
		for i := 0; i+2 < len(f); i++ {
			if f[i] == f[i+1] && f[i] == f[i+2] {
				t.Errorf("fill(%d) = %q has a run of three equal bytes at %d", n, f, i)
			}
		}
		for _, c := range f {
			if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", c) {
				t.Errorf("fill(%d) has the character %q", n, c)
			}
		}
	}
	// Nothing repeats within the longest filler a name needs (57), which a period of 36 would.
	seen := map[rune]bool{}
	for _, c := range workload.ProducerFill(57) {
		if seen[c] {
			t.Errorf("fill(57) = %q repeats %q", workload.ProducerFill(57), c)
		}
		seen[c] = true
	}
}
