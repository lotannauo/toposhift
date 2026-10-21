// Package keys issues, stores and checks the local keys a self-hoster uses to
// reach toposhift without Cloudflare Access.
//
// The package has no listener, no command and no log of its own: [Store] is
// the file of keys and the checks on a presented key, and a caller decides
// where they run. A caller maps every error of [Store.Authenticate] to one
// refusal; the error says which check failed, for the log, and never holds the
// key.
//
// # The key
//
// A key is 55 characters:
//
//	tps_<scope>_<body><check>
//
// where scope is "r" for a read key and "i" for an ingest key, body is 43
// base62 characters (digits, then upper case, then lower case) holding 32
// random bytes as one big-endian number, and check is the CRC-32 (IEEE) of the
// first 49 characters, written as 6 base62 characters. 43 characters hold 256
// bits (62^43 is a little more than 2^256), so the body has all the entropy of
// the bytes. The prefix and the check let a secret scanner match a key with
// few false positives, and let [Parse] refuse a mistyped key (every key with one
// wrong character, and any with a missing or added one) before any file is
// read. The check is not a defence: the key's strength is its 256 random bits.
//
// A key is shown once, by the call that makes it, as a [Secret] that prints as
// a placeholder under every fmt verb, in JSON and in log/slog, and gives up the
// text only to [Secret.Reveal]. Nothing stores or returns it afterwards.
//
// # The file
//
// The keys are the file keys.json of one directory (toposhift's meta
// directory), mode 0600:
//
//	{"version": 1, "keys": [{"id", "scope", "created", "expires", "replaces", "sha256"}]}
//
// "sha256" is the hex SHA-256 of the whole key: no slow hash, because the key
// has 256 random bits and nothing to guess. A key is looked up by comparing
// the presented key's hash with every stored hash in constant time, without
// stopping at a match, so the time does not say which key, or whether one,
// matched. The file is read strictly (unknown members, a bad version, a
// repeated id or hash, a malformed hash, an unknown scope and a size over 4 MiB
// are all refused) and is refused whole, with every key denied, when it is
// writable by its group or others.
//
// Changes are made under a lock of their own, the file keys.lock beside it
// (flock, on Linux and macOS; not the data directory's lock, so a command can
// change the keys while serve runs), and are written to a temporary file and
// renamed over keys.json, so a reader sees the old keys or the new ones and
// never a mixture. A running Store looks at the file's identity and
// modification time at most once a second ([Options.CheckEvery]) and reloads
// it when they changed, so a key revoked by another process stops working
// within that second. A file that cannot be read or fails the checks denies
// every key until it is repaired: a key that was revoked is never honoured
// because the file that revokes it is damaged. A missing file is an empty set.
//
// # Rotation and expiry
//
// A key may carry an expiry, after which it is refused ([ErrExpired]); the
// instant itself is already expired. [Store.Rotate] issues a new key of the
// same scope, recording which key it replaces, and sets the old key's expiry
// to the end of an overlap the caller chooses (zero ends the old key now). It never lengthens a life: an old key that
// expires sooner keeps its own expiry. [Store.Revoke] removes a key outright.
// Keys are not tenants: any key of a scope may do all that scope does.
//
// # Requests
//
// [BearerToken] reads a key from an Authorization header with the Bearer
// scheme and from nowhere else; a key in a query string is never read, since
// URLs are logged.
package keys
