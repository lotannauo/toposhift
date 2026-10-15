package endpoint

import (
	"errors"
	"fmt"
	"net/netip"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// The errors an address is refused with. A refused address is never guessed
// into another form.
var (
	// ErrNotAddress is text that is not one IP address in the canonical
	// syntax [Parse] accepts.
	ErrNotAddress = errors.New("not an IP address")
	// ErrScoped is an address that means something only on one host: a zone,
	// or an unspecified, loopback, link-local unicast or multicast address. It
	// needs a host realm, which no writer has yet.
	ErrScoped = errors.New("the address needs a host realm")
)

// GlobalRealm is the realm of a globally reachable address.
const GlobalRealm = "global"

// clusterRealmPrefix comes before the cluster name in the realm of an address
// that is not globally reachable. The cluster name may hold "/" and ":", which
// is unambiguous after this fixed prefix.
const clusterRealmPrefix = "k8s/"

// resolver builds endpoint identities from the shipped catalog.
var resolver = identity.NewResolver(catalog.Default())

// Parse reads one IP address and returns it in canonical form, an IPv4-mapped
// IPv6 address unmapped. Text that [netip.ParseAddr] refuses, such as an IPv4
// field with a leading zero, wraps [ErrNotAddress]; an address with a zone
// wraps [ErrScoped].
func Parse(s string) (netip.Addr, error) {
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("%w: %w", ErrNotAddress, err)
	}
	if a.Zone() != "" {
		return netip.Addr{}, fmt.Errorf("%q has the zone %q: %w", s, a.Zone(), ErrScoped)
	}
	return a.Unmap(), nil
}

// Realm returns the realm of a, an address asserted by a producer of the named
// cluster: [GlobalRealm] when a lies in no block of the special-purpose table
// and is IPv4 or in the IPv6 global unicast space 2000::/3, else
// "k8s/<cluster>". An IPv4-mapped a is judged as the IPv4 address it maps.
// A cluster name that is not one every reader accepts wraps [store.ErrInvalid];
// an invalid a wraps [ErrNotAddress]; a zone, or an address [ErrScoped] lists,
// wraps ErrScoped.
func Realm(cluster string, a netip.Addr) (string, error) {
	if err := store.ValidClusterName(cluster); err != nil {
		return "", err
	}
	if !a.IsValid() {
		return "", fmt.Errorf("the zero address: %w", ErrNotAddress)
	}
	a = a.Unmap()
	if a.Zone() != "" {
		return "", fmt.Errorf("%s has a zone: %w", a, ErrScoped)
	}
	if a.IsUnspecified() || a.IsLoopback() || a.IsLinkLocalUnicast() || a.IsMulticast() {
		return "", fmt.Errorf("%s is unspecified, loopback, link-local or multicast: %w", a, ErrScoped)
	}
	if notGlobal(a) || (a.Is6() && !globalUnicast.Contains(a)) {
		return clusterRealmPrefix + cluster, nil
	}
	return GlobalRealm, nil
}

// Identity is the identity of the endpoint for the address s, asserted by a
// producer of the named cluster: the realm [Realm] gives, and the address
// [Parse] gives, as [netip.Addr.String] writes it. It is the only way an
// endpoint identity is built.
func Identity(cluster, s string) (identity.Identity, error) {
	a, err := Parse(s)
	if err != nil {
		return identity.Identity{}, err
	}
	realm, err := Realm(cluster, a)
	if err != nil {
		return identity.Identity{}, err
	}
	return resolver.Resolve(catalog.NetworkEndpoint, []identity.Attr{
		{Key: catalog.EndpointRealm, Value: realm},
		{Key: catalog.EndpointAddress, Value: a.String()},
	})
}
