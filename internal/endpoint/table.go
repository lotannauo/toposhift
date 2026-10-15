package endpoint

import "net/netip"

// notGlobalBlocks is every block of the IANA IPv4 and IPv6 Special-Purpose
// Address Registries whose "Globally Reachable" is False, N/A or empty, in the
// registries' order, as published on 2026-10-15. A block nested in another is
// listed as the registry lists it, although the outer block already covers it.
// The blocks whose entry says True are left out, and those nested in a listed
// block are not global here either.
//
// An address in no block is global only if it is also IPv4 or in
// [globalUnicast]: the registries list special blocks, not every block that is
// unique.
//
// The table is part of the canonical form: never edit it in place.
var notGlobalBlocks = []netip.Prefix{
	// IPv4.
	netip.MustParsePrefix("0.0.0.0/8"),          // "This network", RFC 791
	netip.MustParsePrefix("0.0.0.0/32"),         // "This host on this network", RFC 1122
	netip.MustParsePrefix("10.0.0.0/8"),         // Private-Use, RFC 1918
	netip.MustParsePrefix("100.64.0.0/10"),      // Shared Address Space, RFC 6598
	netip.MustParsePrefix("127.0.0.0/8"),        // Loopback, RFC 1122
	netip.MustParsePrefix("169.254.0.0/16"),     // Link Local, RFC 3927
	netip.MustParsePrefix("172.16.0.0/12"),      // Private-Use, RFC 1918
	netip.MustParsePrefix("192.0.0.0/24"),       // IETF Protocol Assignments, RFC 6890
	netip.MustParsePrefix("192.0.0.0/29"),       // IPv4 Service Continuity Prefix, RFC 7335
	netip.MustParsePrefix("192.0.0.8/32"),       // IPv4 dummy address, RFC 7600
	netip.MustParsePrefix("192.0.0.170/32"),     // NAT64/DNS64 Discovery, RFC 8880
	netip.MustParsePrefix("192.0.0.171/32"),     // NAT64/DNS64 Discovery, RFC 8880
	netip.MustParsePrefix("192.0.2.0/24"),       // Documentation (TEST-NET-1), RFC 5737
	netip.MustParsePrefix("192.88.99.0/24"),     // Deprecated (6to4 Relay Anycast), RFC 7526: empty
	netip.MustParsePrefix("192.88.99.2/32"),     // 6a44-relay anycast address, RFC 6751
	netip.MustParsePrefix("192.168.0.0/16"),     // Private-Use, RFC 1918
	netip.MustParsePrefix("198.18.0.0/15"),      // Benchmarking, RFC 2544
	netip.MustParsePrefix("198.51.100.0/24"),    // Documentation (TEST-NET-2), RFC 5737
	netip.MustParsePrefix("203.0.113.0/24"),     // Documentation (TEST-NET-3), RFC 5737
	netip.MustParsePrefix("240.0.0.0/4"),        // Reserved, RFC 1112
	netip.MustParsePrefix("255.255.255.255/32"), // Limited Broadcast, RFC 8190

	// IPv6.
	netip.MustParsePrefix("::1/128"),        // Loopback Address, RFC 4291
	netip.MustParsePrefix("::/128"),         // Unspecified Address, RFC 4291
	netip.MustParsePrefix("::ffff:0:0/96"),  // IPv4-mapped Address, RFC 4291
	netip.MustParsePrefix("64:ff9b:1::/48"), // IPv4-IPv6 Translat., RFC 8215
	netip.MustParsePrefix("100::/64"),       // Discard-Only Address Block, RFC 6666
	netip.MustParsePrefix("100:0:0:1::/64"), // Dummy IPv6 Prefix, RFC 9780
	netip.MustParsePrefix("2001::/23"),      // IETF Protocol Assignments, RFC 2928
	netip.MustParsePrefix("2001::/32"),      // TEREDO, RFC 4380: N/A
	netip.MustParsePrefix("2001:2::/48"),    // Benchmarking, RFC 5180
	netip.MustParsePrefix("2001:10::/28"),   // Deprecated (previously ORCHID), RFC 4843: empty
	netip.MustParsePrefix("2001:db8::/32"),  // Documentation, RFC 3849
	netip.MustParsePrefix("2002::/16"),      // 6to4, RFC 3056: N/A
	netip.MustParsePrefix("3fff::/20"),      // Documentation, RFC 9637
	netip.MustParsePrefix("5f00::/16"),      // Segment Routing (SRv6) SIDs, RFC 9602
	netip.MustParsePrefix("fc00::/7"),       // Unique-Local, RFC 4193
	netip.MustParsePrefix("fe80::/10"),      // Link-Local Unicast, RFC 4291
}

// globalUnicast is the IPv6 global unicast space (RFC 4291, and the IANA IPv6
// Address Space registry). IPv6 outside it, unless refused as scoped, takes the
// cluster's realm whatever the table says. Part of the canonical form.
var globalUnicast = netip.MustParsePrefix("2000::/3")

// notGlobal says a lies in a block of the table. a is unmapped and has no zone.
func notGlobal(a netip.Addr) bool {
	for _, p := range notGlobalBlocks {
		if p.Contains(a) {
			return true
		}
	}
	return false
}
