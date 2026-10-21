package doltengine

import (
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

const sec = int64(time.Second)

func fp(typ catalog.EntityType, b byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = b
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

var (
	pod1, pod2, node1 = fp("k8s.pod", 1), fp("k8s.pod", 2), fp("k8s.node", 3)

	edge1 = engine.EdgeSubject(pod1, node1, catalog.ScheduledOn)
	edge2 = engine.EdgeSubject(pod2, node1, catalog.ScheduledOn)
	edge3 = engine.EdgeSubject(pod1, node1, "part_of")
)

// at is an instant a number of seconds after the epoch.
func at(s int64) time.Time { return time.Unix(s, 0).UTC() }

func observe(s engine.Subject, seq uint64, p lifecycle.Producer, secs, ttl int64) engine.Record {
	return engine.Record{
		Layer: catalog.L2, Subject: s, Producer: p, EventTime: at(secs), Seq: seq,
		Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second, Payload: []byte{byte(seq)},
	}
}

func del(s engine.Subject, seq uint64, p lifecycle.Producer, secs int64) engine.Record {
	return engine.Record{Layer: catalog.L2, Subject: s, Producer: p, EventTime: at(secs), Seq: seq, Kind: lifecycle.Delete}
}

// entity is an existence record of the pod.
func existence(f identity.Fingerprint, seq uint64, p lifecycle.Producer, secs, ttl int64) engine.Record {
	e, _ := catalog.Default().Entity(f.Type())
	return engine.Record{
		Layer: e.Layer(), Subject: engine.EntitySubject(f), Producer: p, EventTime: at(secs), Seq: seq,
		Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second, Payload: []byte{byte(seq)},
	}
}
