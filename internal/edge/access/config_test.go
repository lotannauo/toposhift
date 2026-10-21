package access

import (
	"strings"
	"testing"
	"time"
)

func TestTeamAndAudienceAreChecked(t *testing.T) {
	t.Parallel()
	for _, team := range []string{"acme", "ACME", "acme.cloudflareaccess.com", "Acme.CloudflareAccess.com", "a-b-1", "a", strings.Repeat("a", 63)} {
		v, err := New(Config{Team: team, Audience: "aud"})
		if err != nil {
			t.Errorf("team %q: %v", team, err)
			continue
		}
		if !strings.HasPrefix(v.Issuer(), "https://") || !strings.HasSuffix(v.Issuer(), ".cloudflareaccess.com") || v.Issuer() != strings.ToLower(v.Issuer()) {
			t.Errorf("team %q: issuer %q", team, v.Issuer())
		}
	}
	for _, team := range []string{
		"", ".", "-a", "a-", "a_b", "a b", "a/b", "a:443", "a@b", "a?x", "a#x",
		"https://acme.cloudflareaccess.com", "acme.cloudflareaccess.com/", "acme.cloudflareaccess.com:443",
		"acme.example.com", "evil.com", "a.b.cloudflareaccess.com", "cloudflareaccess.com", ".cloudflareaccess.com",
		"acme.cloudflareaccess.com.evil.com", "evil.com#.cloudflareaccess.com", "acme\n", strings.Repeat("a", 64), "é",
	} {
		if _, err := New(Config{Team: team, Audience: "aud"}); err == nil {
			t.Errorf("team %q was accepted", team)
		}
	}
	for _, aud := range []string{"", "a b", "a\n", "é", strings.Repeat("a", 257)} {
		if _, err := New(Config{Team: "acme", Audience: aud}); err == nil {
			t.Errorf("audience %q was accepted", aud)
		}
	}
}

func TestDurationsDefaultAndRefuseNegatives(t *testing.T) {
	t.Parallel()
	v, err := New(Config{Team: "acme", Audience: "aud"})
	if err != nil {
		t.Fatal(err)
	}
	s := v.s
	if s.skew != 60*time.Second || s.minRefresh != time.Minute || s.refreshInterval != 6*time.Hour || s.maxKeyAge != 24*time.Hour || s.fetchTimeout != 5*time.Second {
		t.Errorf("defaults = %+v", s)
	}
	for _, c := range []Config{
		{Skew: -1},
		{MinRefresh: -1},
		{RefreshInterval: -1},
		{MaxKeyAge: -1},
		{FetchTimeout: -1},
		{RefreshInterval: 2 * time.Hour, MaxKeyAge: time.Hour},
	} {
		c.Team, c.Audience = "acme", "aud"
		if _, err := New(c); err == nil {
			t.Errorf("config %+v was accepted", c)
		}
	}
}

func TestNewMakesNoRequest(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if n := e.certs.calls.Load(); n != 0 {
		t.Errorf("%d requests by New", n)
	}
}

func TestIdentityNameOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		id   Identity
		want string
	}{
		{Identity{Email: "e", CommonName: "c", Subject: "s"}, "e"},
		{Identity{CommonName: "c", Subject: "s"}, "c"},
		{Identity{Subject: "s"}, "s"},
	} {
		if got := tc.id.Name(); got != tc.want {
			t.Errorf("%+v: Name = %q, want %q", tc.id, got, tc.want)
		}
	}
}

func TestReasonOfAnUnrelatedError(t *testing.T) {
	t.Parallel()
	if got := ReasonOf(nil); got != "" {
		t.Errorf("ReasonOf(nil) = %q", got)
	}
	if got := ReasonOf(errUnrelated{}); got != "" {
		t.Errorf("ReasonOf(other) = %q", got)
	}
}

type errUnrelated struct{}

func (errUnrelated) Error() string { return "x" }
