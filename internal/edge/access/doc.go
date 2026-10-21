// Package access verifies the assertion Cloudflare Access adds to every request
// it lets through, so that a listener behind it can tell a request Access
// admitted from one that merely reached the port.
//
// The package has no listener and mounts nothing. [New] takes the team and the
// application's audience tag and returns a [Verifier]; [Verifier.Verify] takes
// the value of the Cf-Access-Jwt-Assertion header and answers with the
// [Identity] the assertion names or an error that matches [ErrUnauthenticated].
// A caller maps every error to one refusal and shows the requester no detail;
// the [Reason] of the error is for the log.
//
// # What is checked
//
// The assertion is a JSON Web Token, and it is trusted only after all of these
// hold, in this order:
//
//  1. It has three parts, each strictly base64url, and is at most 16 KiB.
//  2. Its header names the algorithm RS256, exactly. Any other value, "none"
//     and the HMAC algorithms included, is refused before a key is looked up,
//     so a token cannot choose how it is checked. A header with a "crit" member
//     is refused, and a header's key-bearing members ("jwk", "jku", "x5u") are
//     never read: the only keys are the team's own.
//  3. Its header names a "kid", and the team's certificates hold an RSA key by
//     that id, at least 2048 bits, usable for signatures with RS256.
//  4. The signature over the first two parts verifies under that key
//     (RSASSA-PKCS1-v1_5 with SHA-256).
//  5. Only now are the claims read: "iss" is exactly https://<team>.cloudflareaccess.com;
//     "aud" is the configured tag or a list that holds it; "exp" is present
//     and not past; "nbf", when present, is not in the future. The clock may
//     differ by 60 seconds in either direction.
//  6. It names an identity: an email, a common name or a subject.
//
// Anything else the token carries is ignored. The identity is read from the
// verified claims: the email of a person, or the common name of a service
// token, else its subject. An assertion that verifies but names none of the
// three is refused, since there is nobody to admit.
//
// # Keys
//
// The keys are fetched from https://<team>.cloudflareaccess.com/cdn-cgi/access/certs
// and held in memory. A token whose key id is unknown triggers a fetch, and any
// fetch is made at most once a minute however many requests ask for one, so a
// stream of forged ids cannot turn into a stream of requests to Cloudflare.
// The keys are fetched again, in the same way, when they are six hours old.
// Each fetch replaces the whole set, so a key Cloudflare withdraws stops
// verifying at the next fetch. When a fetch fails the keys already held stay in
// use until they are 24 hours old and then are dropped; with no keys, every
// request is refused. The fetch does not follow redirects, reads at most 1 MiB,
// and accepts a response only with status 200. The certificates URL is built
// from the team name alone: the team must be a single label under
// cloudflareaccess.com, so the host a fetch goes to cannot be chosen by a
// request or by a stray configuration value.
//
// # Not here
//
// The header is the only place the assertion is read: the CF_Authorization
// cookie is ignored, as Cloudflare advises for an application that sits behind
// a proxy. A request that carries no header, or two, is refused. Whether a
// request reached the process through Cloudflare at all is not something a
// header can show, and nothing here replaces a network that admits only the
// tunnel.
package access
