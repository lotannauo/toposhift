// Package vmengine answers the spike's plans from a VictoriaMetrics server over HTTP,
// read only, and holds what is needed to load a stream into one and to check the server
// is the one the baseline is written for.
//
// [Engine] is an engine.Engine whose reads are the rewritten instant queries of package
// vmquery put to the server; a question the store cannot express is refused with the
// typed error of the capability table, never answered from anything else. [Client] is
// the HTTP side: queries, remote write, the flush and merge endpoints, and the server's
// own counters. [Loader] writes the converter's samples to the server and checks the
// counters against the converter's. [CheckFlags] refuses a server whose flags are not
// those of scripts/flags.conf.
//
// The behaviour of the server this relies on was checked against v1.153.0 before any
// figure was read; each statement is marked with the letter of the assumption in package
// vmmodel and says what was found.
package vmengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/bench/victoria/vmwrite"
)

// Client talks to one server. It is safe for concurrent use.
type Client struct {
	base string
	http *http.Client
}

// NewClient returns a client for the server at base, such as http://127.0.0.1:8428. The
// address must be on a loopback interface: the baseline's server has no authentication
// and nothing else is a server it started.
func NewClient(base string) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
		return nil, fmt.Errorf("vmengine: %q is not the address of a server (http://host:port)", base)
	}
	switch u.Hostname() {
	case "127.0.0.1", "::1", "localhost":
	default:
		return nil, fmt.Errorf("vmengine: %q is not a loopback address: the baseline's server listens on loopback only", base)
	}
	return &Client{base: strings.TrimSuffix(base, "/"), http: &http.Client{Timeout: 10 * time.Minute}}, nil
}

// URL is the address of the server.
func (c *Client) URL() string { return c.base }

func (c *Client) do(ctx context.Context, method, path string, form url.Values, body []byte, header map[string]string) ([]byte, int, error) {
	u := c.base + path
	var rd io.Reader
	switch {
	case body != nil:
		rd = bytes.NewReader(body)
	case form != nil && method == http.MethodPost:
		rd = strings.NewReader(form.Encode())
		header = map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	case form != nil:
		u += "?" + form.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return b, resp.StatusCode, err
}

func (c *Client) ok(ctx context.Context, method, path string, form url.Values, want int) ([]byte, error) {
	b, code, err := c.do(ctx, method, path, form, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("vmengine: %s %s: %w", method, path, err)
	}
	if code != want {
		return nil, fmt.Errorf("vmengine: %s %s: status %d: %s", method, path, code, truncate(b))
	}
	return b, nil
}

func truncate(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}

// Instant implements [vmquery.Backend]: the plan's instant query, as a form posted to
// the query endpoint, read as a vector.
func (c *Client) Instant(ctx context.Context, p vmquery.Plan) ([]vmquery.Result, error) {
	b, code, err := c.do(ctx, http.MethodPost, vmquery.QueryPath, p.Request().Form(), nil, nil)
	if err != nil {
		return nil, fmt.Errorf("vmengine: %s: %w", vmquery.QueryPath, err)
	}
	// An error of the query has a JSON body with a status of its own; any other failure
	// is the transport's.
	rs, perr := vmquery.ParseResponse(b)
	if perr != nil {
		return nil, fmt.Errorf("vmengine: query %q at %s (status %d): %w", p.Request().Query, p.At.Format(time.RFC3339Nano), code, perr)
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("vmengine: %s: status %d: %s", vmquery.QueryPath, code, truncate(b))
	}
	return rs, nil
}

// Write posts one remote-write request (assumption (a): the server answers 204 with no
// body).
func (c *Client) Write(ctx context.Context, r vmwrite.Request) error {
	b, code, err := c.do(ctx, http.MethodPost, vmwrite.Path, nil, r.Body, map[string]string{
		"Content-Type":                      vmwrite.ContentType,
		"Content-Encoding":                  vmwrite.ContentEncoding,
		"X-Prometheus-Remote-Write-Version": vmwrite.RemoteWriteVersion,
	})
	if err != nil {
		return fmt.Errorf("vmengine: remote write: %w", err)
	}
	if code != http.StatusNoContent {
		return fmt.Errorf("vmengine: remote write: status %d: %s", code, truncate(b))
	}
	return nil
}

// Flush makes everything written so far readable and the counters current (the server
// holds new samples back for up to a few seconds, and the counters follow).
func (c *Client) Flush(ctx context.Context) error {
	_, err := c.ok(ctx, http.MethodGet, "/internal/force_flush", nil, http.StatusOK)
	return err
}

// ForceMerge asks for every partition's parts to be merged into one. The server answers
// at once and merges only the parts that are on disk then: see [Client.Compact].
func (c *Client) ForceMerge(ctx context.Context) error {
	_, err := c.ok(ctx, http.MethodGet, "/internal/force_merge", nil, http.StatusOK)
	return err
}

// Health reports an error unless the server answers its health check.
func (c *Client) Health(ctx context.Context) error {
	_, err := c.ok(ctx, http.MethodGet, "/health", nil, http.StatusOK)
	return err
}

// Flags returns the flags the server reports as set, by name without the dash, with the
// value unquoted as the server prints it.
func (c *Client) Flags(ctx context.Context) (map[string]string, error) {
	b, err := c.ok(ctx, http.MethodGet, "/flags", nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return parseServerFlags(string(b))
}

func parseServerFlags(text string) (map[string]string, error) {
	out := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok || !strings.HasPrefix(name, "-") {
			return nil, fmt.Errorf("vmengine: a line of /flags that is not -name=value: %q", line)
		}
		if v, err := strconv.Unquote(value); err == nil {
			value = v
		}
		out[strings.TrimPrefix(name, "-")] = value
	}
	return out, nil
}

// HasSeries reports whether the server has a series that matches the selector (such as
// {__name__=~"topo_.*"}) with samples between 2000 and 2050, where every stream of the
// baseline lies. (A lookup over 1970 to 2100 found nothing on the server, though the
// series were there, and one over 2000 to 2050 found them: the range is kept narrow.)
// Call [Client.Flush] first.
func (c *Client) HasSeries(ctx context.Context, selector string) (bool, error) {
	f := url.Values{}
	f.Set("match[]", selector)
	f.Set("start", "946684800") // 2000-01-01
	f.Set("end", "2524608000")  // 2050-01-01
	f.Set("limit", "1")
	b, err := c.ok(ctx, http.MethodGet, "/api/v1/series", f, http.StatusOK)
	if err != nil {
		return false, err
	}
	s := strings.ReplaceAll(string(b), " ", "")
	switch {
	case strings.Contains(s, `"data":[]`):
		return false, nil
	case strings.Contains(s, `"status":"success"`):
		return true, nil
	}
	return false, fmt.Errorf("vmengine: the series endpoint answered %s", truncate(b))
}

// ErrNoMetric marks a counter the server does not report.
var ErrNoMetric = errors.New("vmengine: the server does not report the metric")
