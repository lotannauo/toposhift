package access

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Domain is the suffix every Access team's domain has.
const Domain = "cloudflareaccess.com"

// HeaderName is the request header that carries the assertion.
const HeaderName = "Cf-Access-Jwt-Assertion"

// The defaults of the Config durations.
const (
	DefaultSkew            = 60 * time.Second
	DefaultMinRefresh      = time.Minute
	DefaultRefreshInterval = 6 * time.Hour
	DefaultMaxKeyAge       = 24 * time.Hour
	DefaultFetchTimeout    = 5 * time.Second
)

// Config configures a Verifier. Neither field of the pair Team and Audience
// is a secret, but both are the deployment's own: they come from the
// environment and are never written into the repository.
type Config struct {
	// Team is the Access team: either its name, such as "acme", or its domain,
	// "acme.cloudflareaccess.com". It is lower-cased. Anything else, a scheme,
	// a path, a port, another domain or more than one label before
	// cloudflareaccess.com, is refused.
	Team string
	// Audience is the application's AUD tag, which an assertion's aud must hold.
	Audience string

	// Client fetches the certificates. The default has a timeout and follows no
	// redirect. A client given here is used as it is.
	Client *http.Client
	// Now is the clock; the default is time.Now.
	Now func() time.Time
	// OnFetchError, when set, is called with the error of each failed fetch of
	// the certificates, so that an outage that refuses every request can be
	// logged. It is called by the goroutine that made the fetch, one call at a
	// time, and must not call back into the Verifier. The error names the
	// team's certificates URL and nothing from any assertion.
	OnFetchError func(error)

	// Zero values take the defaults above; negative values are refused.
	Skew            time.Duration // allowed clock difference in exp and nbf
	MinRefresh      time.Duration // the least time between two fetch attempts
	RefreshInterval time.Duration // the age at which keys are fetched again
	MaxKeyAge       time.Duration // the age at which keys are dropped
	FetchTimeout    time.Duration // the bound on one fetch
}

type settings struct {
	host     string
	audience string
	client   *http.Client
	now      func() time.Time
	onError  func(error)

	skew, minRefresh, refreshInterval, maxKeyAge, fetchTimeout time.Duration
}

func (s settings) issuer() string   { return "https://" + s.host }
func (s settings) certsURL() string { return s.issuer() + "/cdn-cgi/access/certs" }

func resolve(c Config) (settings, error) {
	host, err := teamHost(c.Team)
	if err != nil {
		return settings{}, err
	}
	if c.Audience == "" {
		return settings{}, errors.New("access: the audience tag is empty")
	}
	if len(c.Audience) > 256 || strings.ContainsFunc(c.Audience, func(r rune) bool { return r < 0x21 || r > 0x7e }) {
		return settings{}, errors.New("access: the audience tag must be printable ASCII without spaces, at most 256 bytes")
	}
	s := settings{host: host, audience: c.Audience, client: c.Client, now: c.Now, onError: c.OnFetchError}
	if s.client == nil {
		s.client = defaultClient()
	}
	if s.now == nil {
		s.now = time.Now
	}
	for _, d := range []struct {
		name string
		in   time.Duration
		def  time.Duration
		out  *time.Duration
	}{
		{"Skew", c.Skew, DefaultSkew, &s.skew},
		{"MinRefresh", c.MinRefresh, DefaultMinRefresh, &s.minRefresh},
		{"RefreshInterval", c.RefreshInterval, DefaultRefreshInterval, &s.refreshInterval},
		{"MaxKeyAge", c.MaxKeyAge, DefaultMaxKeyAge, &s.maxKeyAge},
		{"FetchTimeout", c.FetchTimeout, DefaultFetchTimeout, &s.fetchTimeout},
	} {
		switch {
		case d.in < 0:
			return settings{}, fmt.Errorf("access: %s is negative", d.name)
		case d.in == 0:
			*d.out = d.def
		default:
			*d.out = d.in
		}
	}
	if s.maxKeyAge < s.refreshInterval {
		return settings{}, errors.New("access: MaxKeyAge is shorter than RefreshInterval")
	}
	return s, nil
}

// teamHost returns the team's host, "<label>.cloudflareaccess.com".
func teamHost(team string) (string, error) {
	t := strings.ToLower(team)
	if t == "" {
		return "", errors.New("access: the team is empty")
	}
	label := strings.TrimSuffix(t, "."+Domain)
	if label == "" || len(label) > 63 || !validLabel(label) {
		return "", fmt.Errorf("access: the team must be a name or <name>.%s, with a name of letters, digits and hyphens", Domain)
	}
	return label + "." + Domain, nil
}

func validLabel(s string) bool {
	if s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

// defaultClient has a timeout and refuses to follow a redirect, so a response
// cannot send the fetch to a host the team name did not name.
func defaultClient() *http.Client {
	return &http.Client{
		Timeout: 2 * DefaultFetchTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
