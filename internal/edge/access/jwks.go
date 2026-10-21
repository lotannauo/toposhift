package access

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"
)

const (
	maxCertsBody = 1 << 20
	maxCertsKeys = 64
	minModulus   = 2048
	maxModulus   = 8192
	minExponent  = 65537
)

// jwk is the part of a JSON Web Key the verifier reads.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// parseCerts reads the certificates document into the usable RSA keys by id.
// A key that cannot verify an RS256 assertion is skipped, not an error: it may
// be a key of another kind that the endpoint also lists. A document with
// the same id twice is refused whole, since it would not say which key an id
// means, and one with no usable key is an error, so that a fetch which returns
// nothing never replaces the keys already held.
func parseCerts(body []byte) (map[string]*rsa.PublicKey, error) {
	var doc struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, errors.New("the certificates are not a JSON object with a keys list")
	}
	if len(doc.Keys) > maxCertsKeys {
		return nil, fmt.Errorf("the certificates list more than %d keys", maxCertsKeys)
	}
	keys := map[string]*rsa.PublicKey{}
	seen := map[string]bool{}
	for _, k := range doc.Keys {
		if k.Kid != "" {
			if seen[k.Kid] {
				return nil, errors.New("the certificates list one key id twice")
			}
			seen[k.Kid] = true
		}
		pk, ok := k.usable()
		if !ok {
			continue
		}
		keys[k.Kid] = pk
	}
	if len(keys) == 0 {
		return nil, errors.New("the certificates hold no usable RSA key")
	}
	return keys, nil
}

// usable returns the key if it is an RSA key with an id that can verify RS256.
func (k jwk) usable() (*rsa.PublicKey, bool) {
	if k.Kty != "RSA" || k.Kid == "" {
		return nil, false
	}
	if k.Alg != "" && k.Alg != "RS256" {
		return nil, false
	}
	if k.Use != "" && k.Use != "sig" {
		return nil, false
	}
	nb, err := base64.RawURLEncoding.Strict().DecodeString(k.N)
	if err != nil {
		return nil, false
	}
	eb, err := base64.RawURLEncoding.Strict().DecodeString(k.E)
	if err != nil || len(eb) == 0 || len(eb) > 4 {
		return nil, false
	}
	n := new(big.Int).SetBytes(nb)
	if n.BitLen() < minModulus || n.BitLen() > maxModulus || n.Bit(0) == 0 {
		return nil, false
	}
	e := 0
	for _, b := range eb {
		e = e<<8 | int(b)
	}
	if e < minExponent || e&1 == 0 || e > 1<<31-1 {
		return nil, false
	}
	return &rsa.PublicKey{N: n, E: e}, true
}

// keyHolder holds the team's keys and decides when to fetch them again.
type keyHolder struct {
	s settings

	// fetchMu serializes fetches and the decision to make one.
	fetchMu sync.Mutex

	mu          sync.Mutex
	keys        map[string]*rsa.PublicKey
	fetchedAt   time.Time // of keys; zero when none
	lastAttempt time.Time // of the last fetch started, successful or not
}

// get returns the key with the id kid. It fetches when the id is unknown or the
// keys are older than the refresh interval, subject to the minimum time between
// fetches; keys older than the maximum age are never returned.
func (h *keyHolder) get(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	if pk, age := h.cached(kid); pk != nil && age < h.s.refreshInterval {
		return pk, nil
	}
	_ = h.refresh(ctx, false)
	pk, age := h.cached(kid)
	if pk != nil && age < h.s.maxKeyAge {
		return pk, nil
	}
	h.mu.Lock()
	none := h.keys == nil || h.s.now().Sub(h.fetchedAt) >= h.s.maxKeyAge
	h.mu.Unlock()
	if none {
		return nil, refuse(ReasonNoKeys, "the team's certificates are not available")
	}
	return nil, refuse(ReasonKey, "no key has the id the assertion names")
}

// cached returns the held key with the id and the age of the held keys.
func (h *keyHolder) cached(kid string) (*rsa.PublicKey, time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.keys == nil {
		return nil, 0
	}
	return h.keys[kid], h.s.now().Sub(h.fetchedAt)
}

// refresh fetches the certificates and replaces the held keys with them. Unless
// force is set it does nothing when the last attempt was less than MinRefresh
// ago, so concurrent callers share one fetch and a run of unknown ids makes at
// most one request a minute. A failed fetch leaves the held keys as they are.
func (h *keyHolder) refresh(ctx context.Context, force bool) error {
	h.fetchMu.Lock()
	defer h.fetchMu.Unlock()
	now := h.s.now()
	h.mu.Lock()
	recent := !h.lastAttempt.IsZero() && now.Sub(h.lastAttempt) < h.s.minRefresh
	if !force && recent {
		h.mu.Unlock()
		return nil
	}
	h.lastAttempt = now
	h.mu.Unlock()

	// The fetch outlives the request that asked for it, so that a client which
	// gives up cannot leave the next minute without keys.
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), h.s.fetchTimeout)
	defer cancel()
	keys, err := h.fetch(fctx)
	if err != nil {
		if h.s.onError != nil {
			h.s.onError(err)
		}
		return err
	}
	h.mu.Lock()
	h.keys, h.fetchedAt = keys, now
	h.mu.Unlock()
	return nil
}

func (h *keyHolder) fetch(ctx context.Context) (map[string]*rsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.s.certsURL(), http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := h.s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("access: fetching the team's certificates: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("access: the team's certificates answered status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCertsBody+1))
	if err != nil {
		return nil, fmt.Errorf("access: reading the team's certificates: %w", err)
	}
	if len(body) > maxCertsBody {
		return nil, fmt.Errorf("access: the team's certificates are larger than %d bytes", maxCertsBody)
	}
	keys, err := parseCerts(body)
	if err != nil {
		return nil, fmt.Errorf("access: %w", err)
	}
	return keys, nil
}
