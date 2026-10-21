package apihttp

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// protect wraps h in the protections of the routes, outermost first: the headers
// every response carries, the local address of the connection, the Host header,
// and the standard library's cross-origin protection. The method, the content
// type and the body are checked by the routes themselves.
func protect(h http.Handler) http.Handler {
	return setHeaders(loopbackOnly(loopbackHost(http.NewCrossOriginProtection().Handler(h))))
}

// setHeaders sets, before anything else can write, what every response says: it
// is never cached, never sniffed into another type, and never sends a referrer
// from the page it is read in. A refusal that is plain text replaces the content
// type.
func setHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Cache-Control", "no-store")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Type", "application/json; charset=utf-8")
		next.ServeHTTP(w, r)
	})
}

// loopbackOnly answers 403 to a request whose connection was not accepted on a
// loopback address. The listener's configured address says only what was asked
// for; this is the address the connection arrived on. A request with no local
// address is refused too.
func loopbackOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLoopbackAddr(r.Context().Value(http.LocalAddrContextKey)) {
			http.Error(w, "Forbidden: the API is served on loopback only", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isLoopbackAddr(v any) bool {
	a, ok := v.(net.Addr)
	if !ok || a == nil {
		return false
	}
	var ip net.IP
	switch a := a.(type) {
	case *net.TCPAddr:
		ip = a.IP
	default:
		host, _, err := net.SplitHostPort(a.String())
		if err != nil {
			return false
		}
		ip = net.ParseIP(host)
	}
	return ip != nil && ip.IsLoopback()
}

// loopbackHost answers 403 to a request whose Host header is not localhost or a
// loopback address, with any port: a page on another site whose name a browser
// has been made to resolve to this machine sends its own name. The sentence
// echoes nothing the client sent.
func loopbackHost(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !loopbackName(r.Host) {
			http.Error(w, "Forbidden: the Host header is not a loopback name", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// loopbackName says host, as a Host header holds it, names this machine: exactly
// localhost, or a loopback IP address, with or without a port.
func loopbackName(host string) bool {
	name, _, err := net.SplitHostPort(host)
	if err != nil {
		name = strings.Trim(host, "[]")
	}
	if name == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(name)
	return err == nil && ip.IsLoopback()
}
