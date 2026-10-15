package backup_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// script is a hand-written stream: each call commits one batch to the live store
// and to the cutter, with the ingest clock the test gives.
type script struct {
	t       testing.TB
	meta    backup.StoreMeta
	live    *memstore.Store
	segs    segment.Store
	cut     *backup.Cutter
	seq     uint64
	written []store.Record
}

func newScript(t testing.TB, policy lifecycle.Policy, h backup.Hooks) *script {
	t.Helper()
	s := &script{t: t, meta: metaFor(policy), segs: newMemSegments()}
	s.live = openMem(t, s.meta)
	opts := testOptions(s.meta)
	opts.TempDir = t.TempDir()
	c, err := backup.NewCutter(ctx, s.segs, prefix, opts)
	if err != nil {
		t.Fatal(err)
	}
	backup.SetHooks(c, h)
	t.Cleanup(func() { _ = c.Close() })
	s.cut = c
	return s
}

// commit numbers the records and commits them as one batch at the clock wall.
func (s *script) commit(wall time.Time, recs ...store.Record) {
	s.t.Helper()
	for i := range recs {
		s.seq++
		recs[i].Seq = s.seq
	}
	if err := s.live.Write(ctx, recs); err != nil {
		s.t.Fatalf("live Write: %v", err)
	}
	s.written = append(s.written, recs...)
	if err := s.cut.Add(recs, wall); err != nil {
		s.t.Fatalf("Add: %v", err)
	}
}

func (s *script) cutNow() {
	s.t.Helper()
	if _, err := s.cut.Cut(ctx); err != nil {
		s.t.Fatalf("Cut: %v", err)
	}
}

// retain retains the live store to asked and logs it.
func (s *script) retain(asked time.Time) {
	s.t.Helper()
	if err := s.live.Retain(ctx, asked); err != nil {
		s.t.Fatal(err)
	}
	if err := s.cut.Retained(asked, horizons(s.live), s.live.LastSeq()); err != nil {
		s.t.Fatal(err)
	}
}

func (s *script) retire() backup.Retirement {
	s.t.Helper()
	ret, err := s.cut.Retire(ctx)
	if err != nil {
		s.t.Fatalf("Retire: %v", err)
	}
	return ret
}

// compareTo restores the backup into an engine and compares it with the live
// store over the given entities.
func (s *script) compareTo(engine string, fps []identity.Fingerprint, span time.Duration, o ...func(*backup.RestoreOptions)) error {
	s.t.Helper()
	r, _ := restoreInto(s.t, s.segs, prefix, engine, s.meta, 7, o...)
	return storetest.CompareRestored(s.live, r, storetest.RestoredOptions{Entities: fps, Written: s.written, Span: span})
}

// fingerprint resolves an entity of the default catalog.
func fingerprint(t testing.TB, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

// base is the start of every scripted stream.
var base = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

func observe(l catalog.Layer, sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration) store.Record {
	return store.Record{Layer: l, Subject: sub, Producer: p, EventTime: at, Kind: lifecycle.Observe, TTL: ttl, Payload: []byte("x")}
}

func deleteOf(l catalog.Layer, sub store.Subject, p lifecycle.Producer, at time.Time) store.Record {
	return store.Record{Layer: l, Subject: sub, Producer: p, EventTime: at, Kind: lifecycle.Delete}
}
