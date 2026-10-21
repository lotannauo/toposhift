package xtengine

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func fpOf(typ catalog.EntityType, b byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = b
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

// The subjects of the test streams: two entities and edges between them, in layer L2.
var (
	pod1, pod2, node1 = fpOf("k8s.pod", 1), fpOf("k8s.pod", 2), fpOf("k8s.node", 3)

	edgeA = engine.EdgeSubject(pod1, node1, catalog.ScheduledOn)
	edgeB = engine.EdgeSubject(pod2, node1, catalog.ScheduledOn)
)

// sec is the instant s seconds after the epoch.
func sec(s int64) time.Time { return time.Unix(s, 0).UTC() }

func edgeRec(seq uint64, subject engine.Subject, p lifecycle.Producer, s int64, ttl int64) engine.Record {
	return engine.Record{
		Layer: catalog.L2, Subject: subject, Producer: p, EventTime: sec(s), Seq: seq,
		Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second, Payload: []byte{byte(seq)},
	}
}

func observeA(seq uint64, p lifecycle.Producer, s int64, ttl int64) engine.Record {
	return edgeRec(seq, edgeA, p, s, ttl)
}

func podRec(seq uint64, s int64) engine.Record {
	return engine.Record{
		Layer: catalog.L2, Subject: engine.EntitySubject(pod1), Producer: "a", EventTime: sec(s), Seq: seq,
		Kind: lifecycle.Observe, Payload: []byte{byte(seq)},
	}
}

func orNil(err error) error {
	if err == nil {
		return errors.New("no error")
	}
	return err
}

// beginTimes are the system times of the transactions a connection opened, in order, as
// the microseconds the statements wrote them.
func beginTimes(stmts []loggedStmt) []string {
	var out []string
	for _, s := range stmts {
		if rest, ok := strings.CutPrefix(s.sql, "BEGIN READ WRITE WITH (SYSTEM_TIME = TIMESTAMP '"); ok {
			out = append(out, strings.TrimSuffix(rest, "')"))
		}
	}
	return out
}

// sysLit is the literal of the system time of the n-th transaction at the base.
func sysLit(base time.Time, n int) string {
	return base.UTC().Add(time.Duration(n) * time.Microsecond).Format("2006-01-02T15:04:05.000000Z")
}

var defaultBase = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func describe(stmts []loggedStmt) string {
	var b strings.Builder
	for _, s := range stmts {
		fmt.Fprintf(&b, "%.100s %v\n", s.sql, s.args)
	}
	return b.String()
}
