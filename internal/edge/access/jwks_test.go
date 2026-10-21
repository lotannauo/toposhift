package access

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// tokenFor signs the default claims with key under the kid.
func (e *env) tokenFor(t testing.TB, key *rsa.PrivateKey, kid string) string {
	t.Helper()
	h := e.header()
	h["kid"] = kid
	return signRS256(t, key, signingInput(t, h, e.payload()))
}

func TestUnknownKeyIdsFetchAtMostOnceAMinute(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	good := e.token(t, nil)
	if _, err := e.verify(good); err != nil {
		t.Fatal(err)
	}
	if n := e.certs.calls.Load(); n != 1 {
		t.Fatalf("%d fetches for the first assertion, want 1", n)
	}
	// A known key never fetches again inside the refresh interval.
	for range 5 {
		if _, err := e.verify(good); err != nil {
			t.Fatal(err)
		}
	}
	if n := e.certs.calls.Load(); n != 1 {
		t.Fatalf("%d fetches after repeated known keys, want 1", n)
	}

	// A stream of forged ids is refused, and fetches nothing inside a minute.
	e.clock.Advance(time.Second)
	for i := range 20 {
		forged := e.tokenFor(t, e.b, "forged-"+string(rune('a'+i)))
		if _, err := e.verify(forged); ReasonOf(err) != ReasonKey {
			t.Fatalf("forged id %d: %v, want an unknown key refusal", i, err)
		}
	}
	if n := e.certs.calls.Load(); n != 1 {
		t.Fatalf("%d fetches for unknown ids a second after the last, want 1", n)
	}

	// 59 s after the first attempt: still none. At 60 s: one, and one only.
	e.clock.Advance(58 * time.Second)
	if _, err := e.verify(e.tokenFor(t, e.b, "forged")); ReasonOf(err) != ReasonKey {
		t.Fatal(err)
	}
	if n := e.certs.calls.Load(); n != 1 {
		t.Fatalf("%d fetches at 59 s, want 1", n)
	}
	e.clock.Advance(time.Second)
	for range 5 {
		if _, err := e.verify(e.tokenFor(t, e.b, "forged")); ReasonOf(err) != ReasonKey {
			t.Fatal(err)
		}
	}
	if n := e.certs.calls.Load(); n != 2 {
		t.Fatalf("%d fetches at 60 s, want 2", n)
	}
}

func TestAKeyRotatedInIsFetchedOnFirstUse(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(2 * time.Minute)
	e.certs.set(jwkOf("k1", &e.a.PublicKey), jwkOf("k2", &e.b.PublicKey))
	if _, err := e.verify(e.tokenFor(t, e.b, "k2")); err != nil {
		t.Fatalf("a key published after the first fetch: %v", err)
	}
	// Both keys verify now, each only under its own id.
	if _, err := e.verify(e.tokenFor(t, e.a, "k1")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.verify(e.tokenFor(t, e.a, "k2")); ReasonOf(err) != ReasonSignature {
		t.Fatalf("key A under B's id: %v, want a signature refusal", err)
	}
}

func TestAWithdrawnKeyStopsVerifyingAtTheNextFetch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.certs.set(jwkOf("k1", &e.a.PublicKey), jwkOf("k2", &e.b.PublicKey))
	if _, err := e.verify(e.tokenFor(t, e.a, "k1")); err != nil {
		t.Fatal(err)
	}
	e.certs.set(jwkOf("k2", &e.b.PublicKey))

	// Inside the refresh interval the held set is still used.
	e.clock.Advance(time.Hour)
	if _, err := e.verify(e.tokenFor(t, e.a, "k1")); err != nil {
		t.Fatalf("k1 inside the interval: %v", err)
	}
	// At six hours the set is fetched again and k1 is gone.
	e.clock.Advance(5 * time.Hour)
	if _, err := e.verify(e.tokenFor(t, e.a, "k1")); ReasonOf(err) != ReasonKey {
		t.Fatalf("k1 after the refresh: %v, want an unknown key refusal", err)
	}
	if _, err := e.verify(e.tokenFor(t, e.b, "k2")); err != nil {
		t.Fatal(err)
	}
}

func TestKeysAreFetchedAgainAtSixHours(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(6*time.Hour - time.Second)
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Fatal(err)
	}
	if n := e.certs.calls.Load(); n != 1 {
		t.Fatalf("%d fetches just inside six hours, want 1", n)
	}
	e.clock.Advance(time.Second)
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Fatal(err)
	}
	if n := e.certs.calls.Load(); n != 2 {
		t.Fatalf("%d fetches at six hours, want 2", n)
	}
}

func TestWithTheCertificatesDownNothingUnknownIsAdmitted(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.certs.setStatus(http.StatusInternalServerError)

	// No keys have ever been fetched: every assertion is refused, even a good one.
	if _, err := e.verify(e.token(t, nil)); ReasonOf(err) != ReasonNoKeys {
		t.Fatalf("a good assertion with no keys: %v, want a no keys refusal", err)
	}
	// So is one under an unknown id (the case of a forged kid during an outage).
	e.clock.Advance(2 * time.Minute)
	if _, err := e.verify(e.tokenFor(t, e.b, "k2")); ReasonOf(err) != ReasonNoKeys {
		t.Fatalf("unknown id with the certificates down: %v, want a no keys refusal", err)
	}
	// When the endpoint recovers, the next attempt after a minute succeeds.
	e.certs.setStatus(0)
	if _, err := e.verify(e.token(t, nil)); ReasonOf(err) != ReasonNoKeys {
		t.Fatalf("inside the minute after a failed attempt: %v", err)
	}
	e.clock.Advance(time.Minute)
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
}

func TestHeldKeysSurviveAnOutageForADayAndNoLonger(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Fatal(err)
	}
	e.certs.setStatus(http.StatusBadGateway)

	// Past six hours the refresh fails and the held key keeps verifying a known id,
	// while an unknown id is still refused.
	e.clock.Advance(7 * time.Hour)
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Fatalf("held key during an outage: %v", err)
	}
	if _, err := e.verify(e.tokenFor(t, e.b, "k2")); ReasonOf(err) != ReasonKey {
		t.Fatalf("unknown id during an outage with keys held: %v, want an unknown key refusal", err)
	}
	// At 24 hours from the fetch the key is dropped.
	e.clock.Advance(17*time.Hour - time.Second)
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Fatalf("held key just under a day: %v", err)
	}
	e.clock.Advance(time.Second)
	if _, err := e.verify(e.token(t, nil)); ReasonOf(err) != ReasonNoKeys {
		t.Fatalf("held key at a day: %v, want a no keys refusal", err)
	}
	// And a recovery brings keys back.
	e.certs.setStatus(0)
	e.clock.Advance(time.Minute)
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
}

func TestAFailedFetchNeverReplacesTheHeldKeys(t *testing.T) {
	t.Parallel()
	for name, set := range map[string]func(c *certs, e *env){
		"status":         func(c *certs, _ *env) { c.setStatus(http.StatusServiceUnavailable) },
		"not JSON":       func(c *certs, _ *env) { c.setBody([]byte("<html>")) },
		"no keys":        func(c *certs, _ *env) { c.setBody([]byte(`{"keys":[]}`)) },
		"only unusable":  func(c *certs, _ *env) { c.setBody([]byte(`{"keys":[{"kty":"EC","kid":"x"}]}`)) },
		"duplicated kid": func(c *certs, e *env) { c.set(jwkOf("k1", &e.a.PublicKey), jwkOf("k1", &e.b.PublicKey)) },
		"too large": func(c *certs, _ *env) {
			c.setBody([]byte(`{"keys":[],"pad":"` + strings.Repeat("a", maxCertsBody) + `"}`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			if _, err := e.verify(e.token(t, nil)); err != nil {
				t.Fatal(err)
			}
			set(e.certs, e)
			e.clock.Advance(7 * time.Hour) // forces a fetch attempt
			if _, err := e.verify(e.token(t, nil)); err != nil {
				t.Fatalf("held key after a bad document: %v", err)
			}
			if n := e.certs.calls.Load(); n != 2 {
				t.Fatalf("%d fetches, want the attempt to have been made (2)", n)
			}
			// The key B would verify if the bad document had been taken.
			if _, err := e.verify(e.tokenFor(t, e.b, "k1")); ReasonOf(err) != ReasonSignature {
				t.Fatalf("key B under k1: %v, want a signature refusal", err)
			}
		})
	}
}

func TestConcurrentRequestsShareOneFetch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	gate := make(chan struct{})
	e.certs.hook = func() { <-gate }
	tok := e.token(t, nil)

	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.verify(tok)
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond) // let them queue on the one fetch
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	if n := e.certs.calls.Load(); n != 1 {
		t.Errorf("%d fetches for 40 concurrent first requests, want 1", n)
	}
}

func TestRefreshFetchesNowAndReportsFailure(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if err := e.v.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.v.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := e.certs.calls.Load(); n != 2 {
		t.Errorf("%d fetches for two Refresh calls, want 2 (it is not rate limited)", n)
	}
	e.certs.setStatus(http.StatusInternalServerError)
	err := e.v.Refresh(context.Background())
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("Refresh against a failing endpoint: %v", err)
	}
	// A failed start-up fetch leaves the verifier working once keys are there.
	e.certs.setStatus(0)
	if _, err := e.verify(e.token(t, nil)); err != nil {
		t.Errorf("after a failed Refresh: %v", err)
	}
}

func TestTheFetchGoesToTheTeamsCertsURLOnly(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if got, want := e.v.CertsURL(), "https://acme.cloudflareaccess.com/cdn-cgi/access/certs"; got != want {
		t.Errorf("CertsURL = %q, want %q", got, want)
	}
	if got, want := e.v.Issuer(), "https://acme.cloudflareaccess.com"; got != want {
		t.Errorf("Issuer = %q, want %q", got, want)
	}
	var seen []string
	c := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seen = append(seen, r.Method+" "+r.URL.String())
		rec := httptest.NewRecorder()
		e.certs.ServeHTTP(rec, r)
		return rec.Result(), nil
	})}
	v, err := New(Config{Team: "Acme.CloudflareAccess.com", Audience: testAudience, Client: c, Now: e.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(context.Background(), e.token(t, nil)); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] != "GET https://acme.cloudflareaccess.com/cdn-cgi/access/certs" {
		t.Errorf("requests = %q", seen)
	}
}

func TestTheDefaultClientFollowsNoRedirect(t *testing.T) {
	t.Parallel()
	hit := make(chan string, 4)
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit <- r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer other.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/certs", http.StatusFound)
	}))
	defer redirector.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, redirector.URL, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := defaultClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Errorf("status = %d, want the 302 returned unfollowed", resp.StatusCode)
	}
	select {
	case p := <-hit:
		t.Errorf("the redirect target was requested (%s)", p)
	default:
	}
}

func TestOnlyUsableKeysAreTaken(t *testing.T) {
	t.Parallel()
	a, _, _ := testKeys(t)
	small, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	good := jwkOf("good", &a.PublicKey)
	with := func(k map[string]any, kv ...any) map[string]any {
		c := map[string]any{}
		for key, v := range k {
			c[key] = v
		}
		for i := 0; i < len(kv); i += 2 {
			if kv[i+1] == nil {
				delete(c, kv[i].(string))
			} else {
				c[kv[i].(string)] = kv[i+1]
			}
		}
		return c
	}
	e3 := jwkOf("e3", &rsa.PublicKey{N: a.N, E: 3})
	even := with(good, "kid", "even", "n", b64(new(big.Int).Lsh(a.N, 1).Bytes()))
	huge := with(good, "kid", "huge", "n", b64(new(big.Int).Lsh(big.NewInt(1), 8300).Bytes()))
	bad := []map[string]any{
		jwkOf("small", &small.PublicKey),
		e3,
		even,
		huge,
		with(good, "kid", "ec", "kty", "EC"),
		with(good, "kid", "enc", "use", "enc"),
		with(good, "kid", "rs384", "alg", "RS384"),
		with(good, "kid", "nokid", "kid", nil),
		with(good, "kid", "badn", "n", "!!"),
		with(good, "kid", "bade", "e", "!!"),
		with(good, "kid", "noexp", "e", ""),
		with(good, "kid", "bigE", "e", b64([]byte{1, 0, 0, 0, 1})),
	}
	body := jsonBytes(t, map[string]any{"keys": append([]map[string]any{good}, bad...)})
	got, err := parseCerts(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got["good"] == nil {
		ids := []string{}
		for id := range got {
			ids = append(ids, id)
		}
		t.Fatalf("took keys %v, want only \"good\"", ids)
	}
	if got["good"].N.Cmp(a.N) != 0 || got["good"].E != a.E {
		t.Error("the key taken is not the one listed")
	}

	// A key with no use or alg member is taken; those members are optional.
	plain := with(good, "alg", nil, "use", nil)
	if _, err := parseCerts(jsonBytes(t, map[string]any{"keys": []any{plain}})); err != nil {
		t.Errorf("a key without alg and use: %v", err)
	}

	tooMany := make([]map[string]any, maxCertsKeys+1)
	for i := range tooMany {
		tooMany[i] = with(good, "kid", strings.Repeat("k", i+1))
	}
	if _, err := parseCerts(jsonBytes(t, map[string]any{"keys": tooMany})); err == nil {
		t.Error("a document of more than the allowed number of keys was taken")
	}
}

func TestFailedFetchesAreReported(t *testing.T) {
	t.Parallel()
	a, _, _ := testKeys(t)
	c := &certs{}
	c.set(jwkOf("k1", &a.PublicKey))
	clk := newClock()
	var mu sync.Mutex
	var got []error
	v, err := New(Config{
		Team: testTeam, Audience: testAudience, Client: c.client(), Now: clk.Now,
		OnFetchError: func(err error) { mu.Lock(); got = append(got, err); mu.Unlock() },
	})
	if err != nil {
		t.Fatal(err)
	}
	c.setStatus(http.StatusBadGateway)
	e := &env{v: v, certs: c, clock: clk, a: a}
	tok := e.token(t, nil)
	for range 3 { // three requests inside a minute make one attempt
		if _, err := v.Verify(context.Background(), tok); ReasonOf(err) != ReasonNoKeys {
			t.Fatal(err)
		}
	}
	mu.Lock()
	n := len(got)
	var msg string
	if n > 0 {
		msg = got[0].Error()
	}
	mu.Unlock()
	if n != 1 || !strings.Contains(msg, "502") {
		t.Fatalf("reported %d errors, first %q; want one naming status 502", n, msg)
	}
	if strings.Contains(msg, tok) {
		t.Error("the report holds the assertion")
	}
	c.setStatus(0)
	clk.Advance(time.Minute)
	if _, err := v.Verify(context.Background(), tok); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Errorf("a successful fetch was reported as an error: %d reports", len(got))
	}
}
