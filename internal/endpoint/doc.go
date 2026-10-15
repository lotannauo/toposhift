// Package endpoint is the one canonical form of a network address, and the only
// way an endpoint identity is built.
//
// An endpoint (catalog type topo.endpoint) is an address within its realm, the
// address space the address is unique in. Its identity has two keys: the realm
// and the address. Every writer that asserts an endpoint, and every read that
// looks one up, goes through [Identity], so one address never becomes two
// entities because two callers spelled it differently.
//
// # The canonical form
//
// The identity layer takes every value exactly: nothing is trimmed, case-folded
// or normalized. An address is the one exception, and it is made here, before
// identity, never inside it:
//
//   - [Parse] reads the text with [netip.ParseAddr], so an IPv4 field with a
//     leading zero is refused, and IPv6 is accepted in any case and with or
//     without zero compression;
//   - a zone ("fe80::1%eth0") is refused: it names an interface of one host;
//   - an IPv4-mapped IPv6 address is unmapped: "::ffff:10.0.0.1" is 10.0.0.1;
//   - the address key is [netip.Addr.String] of the result: dotted decimal for
//     IPv4, lowercase RFC 5952 text for IPv6.
//
// # The realm
//
// [Realm] gives "global" for an address that is globally reachable, and
// "k8s/<cluster>" for any other, where <cluster> is exactly the cluster part of
// the asserting producer's name, the default "default" included. Private
// address ranges repeat across clusters (every k3s cluster hands out pod
// addresses from 10.42.0.0/16), so two clusters' 10.42.0.5 are two endpoints. A
// private address shared by two clusters in one network is split by cluster,
// which can only miss a match, never make a false one.
//
// An address is global only when both hold: it lies in no block of a frozen copy
// of the IANA IPv4 and IPv6 Special-Purpose Address Registries whose "Globally
// Reachable" is False, N/A or empty, and it is IPv4 or lies in 2000::/3, the
// IPv6 global unicast space. IPv6 outside 2000::/3 that no block lists, such as
// ::2, the IPv4-compatible ::10.42.0.5, deprecated site-local fec0::/10,
// fe00::/9, unassigned space, and the NAT64 prefix 64:ff9b::/96 that each
// network translates on its own, is not known to be unique, so it takes the
// cluster's realm. A block whose registry entry says True but that lies inside
// such a block (192.0.0.9/32 in 192.0.0.0/24) is not global either. Both rules
// err on the side that splits. The table and the 2000::/3 rule are part of the
// canonical form: a change to either is a new identity for every address it
// moves.
//
// The unspecified, loopback, link-local unicast and multicast addresses are
// refused with [ErrScoped]: they mean something only on one host, and need a
// host realm that no writer has yet.
//
// The canonical form, the table included, is frozen by
// testdata/canonical_v1.json, which is never regenerated to change an output:
// a change to the form is a new version of that file and a migration of stored
// endpoints. It changes no byte of the identity encoding: an endpoint identity
// is an ordinary identity of two string keys.
package endpoint
