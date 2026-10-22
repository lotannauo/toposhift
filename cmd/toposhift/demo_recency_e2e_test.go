package main

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/demo"
)

// The demo shows, beside each caller and callee of a service, how long the log
// proves its dependency has gone unseen. checkout calls legacy-pay until 02:10 and
// its last sighting is at 02:10:20Z; the demo's expiry is 15 minutes, so the edge
// ends at 02:25:20Z, and the extension interval of its record is 7m30s. Asked over a
// window that ends inside those last 15 minutes, the answer must show a positive
// bound that is no larger than the silence the world says there was, and short of it
// by at most the extension interval; asked over a window that ends after the edge
// did, it shows none. The true sightings come from the world alone (the oracle's
// rules for the service graph), not from the stores.
func TestDemoShowsHowLongADependencyHasGoneUnseen(t *testing.T) {
	o := newOracle(t)
	q := askedAbout(o)
	s := startDemo(t)

	checkout := q.story.Service("shop", "checkout")
	ask := func(to time.Time, extra map[string]any) map[string]any {
		t.Helper()
		args := map[string]any{
			"service": checkout.ID, "from": q.context.from.Format(time.RFC3339), "to": to.Format(time.RFC3339),
			"limit": contextLimit,
		}
		for k, v := range extra {
			args[k] = v
		}
		res := mcpOK(t, s.base, "service_context", args)
		if res.IsError || res.Structured == nil {
			t.Fatalf("service_context to %s: %q", to.Format(time.RFC3339), res.Text)
		}
		return res.Structured
	}

	// A call is sighted at every flush of the service graph from its first to the
	// one after its last call, flushPast after each whole minute of the anchor's.
	sightings := func(c demo.Call, at time.Time) time.Time {
		grid := o.story.Anchor.Add(flushPast)
		lastEver := o.callIv(c).to.Add(-expiry)
		onGrid := grid.Add(at.Sub(grid) / time.Minute * time.Minute)
		if onGrid.Before(lastEver) {
			return onGrid
		}
		return lastEver
	}
	callOf := func(name, namespace string, callerSide bool) demo.Call {
		t.Helper()
		for _, c := range o.w.Calls {
			if c.Database {
				continue
			}
			if callerSide && c.Client == name && c.ClientNamespace == namespace && c.Server == "checkout" {
				return c
			}
			if !callerSide && c.Server == name && c.ServerNamespace == namespace && c.Client == "checkout" {
				return c
			}
		}
		t.Fatalf("no call between checkout and %s/%s", namespace, name)
		return demo.Call{}
	}

	const extension = expiry / 2 // 7m30s, the coalescer's half of the instance TTL
	const halfLife = extension / 4
	// A window that ends five minutes after the last call of legacy-pay (02:10) and
	// well inside its 15 minutes of liveness. (The margin check says no call between services began or ended near it; pods do not matter, since the hop names none.)
	to := q.story.Migrate.Add(20 * time.Minute)
	o.checkServiceMargin("recency", "the window end", to)
	at := to.Add(-time.Nanosecond)
	if end := q.story.LegacyCallEnds; !end.After(to) {
		t.Fatalf("the edge to legacy-pay ends at %s, not after the window's end %s", fmtTime(end), fmtTime(to))
	}

	res := ask(to, nil)
	seen := map[string]bool{}
	for _, side := range []struct {
		key        string
		callerSide bool
	}{{"callers", true}, {"callees", false}} {
		for _, e := range arr(res[side.key]) {
			h := obj(e)
			svc := obj(h["service"])
			name, namespace := str(svc["name"]), str(svc["namespace"])
			seen[namespace+"/"+name] = true
			c := callOf(name, namespace, side.callerSide)
			rec := obj(h["recency"])
			if rec == nil {
				t.Errorf("%s/%s has no recency over a window that ends inside its edge", namespace, name)
				continue
			}
			if str(rec["basis"]) != "derived" || str(rec["subject"]) != "derived" || str(rec["rule"]) != "decay.recency" {
				t.Errorf("%s/%s: recency labels %v", namespace, name, rec)
			}
			if got, want := str(rec["producer"]), "svcgraph/"+demo.ClusterName; got != want {
				t.Errorf("%s/%s: producer %q, want %q", namespace, name, got, want)
			}
			lastSeen, err := time.Parse(time.RFC3339Nano, str(rec["last_seen"]))
			if err != nil {
				t.Fatal(err)
			}
			silent, err := time.ParseDuration(str(rec["silent_at_least"]))
			if err != nil {
				t.Fatal(err)
			}
			confidence, _ := rec["confidence"].(float64)

			truth := sightings(c, at)
			trueSilence := at.Sub(truth)
			// The record's last instant is a sighting's, no later than the last one, and at
			// most the extension interval earlier.
			if lastSeen.After(truth) || truth.Sub(lastSeen) > extension {
				t.Errorf("%s/%s: last_seen %s, the last sighting at or before the window's last instant was %s", namespace, name, fmtTime(lastSeen), fmtTime(truth))
			}
			// The bound is the silence of the record less the extension interval, and
			// it is sound: never above the truth, and short of it by at most the interval.
			if want := max(0, at.Sub(lastSeen)-extension); silent != want {
				t.Errorf("%s/%s: silent_at_least %s, want %s from last_seen %s", namespace, name, silent, want, fmtTime(lastSeen))
			}
			if silent > trueSilence || trueSilence-silent > extension {
				t.Errorf("%s/%s: silent_at_least %s against a true silence of %s (extension interval %s)", namespace, name, silent, trueSilence, extension)
			}
			if want := math.Round(math.Exp2(-float64(silent)/float64(halfLife))*1e4) / 1e4; confidence != want {
				t.Errorf("%s/%s: confidence %v for a bound of %s, want %v", namespace, name, confidence, silent, want)
			}
			if name == "legacy-pay" {
				// The checkout to legacy-pay edge: last sighted at 02:10:20Z, and the window ends
				// at 02:19:59.999999999Z, 9m39.999999999s later.
				if want := q.story.LegacyCallEnds.Add(-expiry); !truth.Equal(want) {
					t.Errorf("the world's last sighting of legacy-pay is %s, the story's is %s", fmtTime(truth), fmtTime(want))
				}
				if silent <= 0 || silent > trueSilence || trueSilence != 9*time.Minute+40*time.Second-time.Nanosecond {
					t.Errorf("legacy-pay: a bound of %s against a silence of %s, want a positive bound no larger than 9m39.999999999s", silent, trueSilence)
				}
				if confidence <= 0 || confidence >= 1 {
					t.Errorf("legacy-pay: confidence %v, want strictly between 0 and 1", confidence)
				}
				t.Logf("legacy-pay at %s: last_seen %s, silent_at_least %s of a true %s, confidence %v",
					fmtTime(at), str(rec["last_seen"]), silent, trueSilence, confidence)
			}
		}
	}
	for _, key := range []string{"shop/frontend", "shop/cart", "payments/legacy-pay", "payments/payments-api"} {
		if !seen[key] {
			t.Errorf("%s is not among the hop of checkout: %v", key, seen)
		}
	}

	// Asked again at its token, the same bytes: the recency is a function of the log
	// at the token, not of when it is asked.
	again := ask(to, map[string]any{"asof": str(res["token"])})
	if a, b := encode(t, res), encode(t, again); !bytes.Equal(a, b) {
		t.Errorf("service_context at asof %s differs from the answer without it:\n%s", str(res["token"]), firstDifference(a, b))
	}

	// Over the window of the end-to-end test, which ends after the edge to legacy-pay
	// did, that edge has no recency, and the ones that reach its end have.
	full := ask(q.context.to, nil)
	for _, e := range arr(full["callees"]) {
		h := obj(e)
		name := str(obj(h["service"])["name"])
		spans := arr(h["spans"])
		endedEarly := len(spans) > 0 && obj(spans[len(spans)-1])["end_reason"] != nil
		if hasRecency := obj(h["recency"]) != nil; hasRecency == endedEarly {
			t.Errorf("%s: a span that ends early is %t, and it has a recency: %t", name, endedEarly, hasRecency)
		}
		if name == "legacy-pay" && !endedEarly {
			t.Errorf("legacy-pay did not end inside the full window: %v", spans)
		}
	}

	if code := s.stop(t); code != 0 {
		t.Errorf("exit %d after the signal, want 0:\n%s", code, s.stderr)
	}
}
