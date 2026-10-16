package mcphttp

import (
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// A payload that is not a JSON object is withheld whole, with a note, and the
// answer still fits its output schema: a description is an empty object, never
// null, or the SDK refuses the answer as a protocol error.
func TestPayloadsThatAreNotObjectsFitTheOutputSchema(t *testing.T) {
	policy := lifecycle.Policy{Rank: rankObjs, BootKey: lifecycle.BootID}
	for _, tc := range []struct {
		name string
		open func(*testing.T) store.Store
	}{
		{"memstore", func(t *testing.T) store.Store { return openMem(t, memstore.Options{Policy: policy}) }},
		{"pebblestore", func(t *testing.T) store.Store {
			o := pebblestore.DefaultOptions()
			o.Policy = policy
			st, err := pebblestore.Open(t.TempDir(), o)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			return st
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := tc.open(t)
			array := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-array")
			str := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-string")
			newFeed(t, st).write(
				observe(ent(array), objs, hm(10, 0), 0, `[1,2]`),
				observe(ent(str), objs, hm(10, 0), 0, `"web-0"`),
				observe(ent(array), objs, hm(10, 15), 0, `[3]`),
				observe(ent(str), objs, hm(10, 15), 0, `"web-1"`),
			)
			srv := newServer(t, rankObjs)
			if err := srv.attach(st, true); err != nil {
				t.Fatal(err)
			}
			cs := connect(t, srv)

			for _, fp := range []string{array.String(), str.String()} {
				m := answer(t, "entity_history", callTool(t, cs, "entity_history",
					map[string]any{"entity": fp, "from": "2026-01-01T10:05:00Z", "to": w1030}))
				ex, _ := m["existence"].(map[string]any)
				if ex == nil {
					t.Fatalf("entity_history of %s: no existence: %v", fp, m)
				}
				for _, key := range []string{"description_at_start", "description_at_end"} {
					d, _ := ex[key].(map[string]any)
					if d == nil {
						t.Errorf("entity_history of %s: no %s: %v", fp, key, ex)
						continue
					}
					if p, ok := d["payload"].(map[string]any); !ok || len(p) != 0 {
						t.Errorf("entity_history of %s: %s payload %v, want the empty object", fp, key, d["payload"])
					}
					if note, _ := d["note"].(string); !strings.Contains(note, "not a JSON object") {
						t.Errorf("entity_history of %s: %s note %q, want the not-an-object note", fp, key, note)
					}
				}

				answer(t, "what_changed", callTool(t, cs, "what_changed",
					map[string]any{"scope": []string{fp}, "from": w10, "to": w1030}))
			}
		})
	}
}
