package store

import (
	"encoding/binary"
	"fmt"
)

// The cluster name is the name a deployment gives the cluster it reads, which a
// producer name such as "k8scluster/<cluster>" carries after its prefix. A store
// records the name it was started with, so that a changed name is noticed (see the
// store packages' ClusterName and RecordClusterName), and the name is bounded by
// the producer name it becomes part of.
const (
	// producerPrefixLen is the length of the longest producer prefix the cluster
	// name follows: "k8sobjects/" and "k8scluster/" have the same length.
	producerPrefixLen = len("k8sobjects/")

	// MaxClusterNameFormat is the longest cluster name any reader accepts, in
	// bytes: the longest producer name a reader accepts, less the prefix. The value
	// codec and its golden vectors are bound by it, so it does not move when
	// [MaxClusterNameLen] does.
	MaxClusterNameFormat = MaxProducerFormat - producerPrefixLen

	// MaxClusterNameLen is the longest cluster name a store writes, in bytes: the
	// longest producer name a store writes, less the prefix. A configured name over
	// it is refused at boot, and [CheckClusterNameLimit] says so.
	MaxClusterNameLen = MaxProducerLen - producerPrefixLen
)

// clusterNameVersion is the leading byte of a recorded cluster name.
const clusterNameVersion = 1

// ValidClusterName checks a cluster name against the format every reader accepts:
// 1 to [MaxClusterNameFormat] bytes of ASCII from [A-Za-z0-9._:/-], exact (no
// trimming, no case-folding). It does not check [MaxClusterNameLen]; see
// [CheckClusterNameLimit]. Every error wraps [ErrInvalid].
func ValidClusterName(name string) error {
	if name == "" {
		return fmt.Errorf("cluster name is empty: %w", ErrInvalid)
	}
	if len(name) > MaxClusterNameFormat {
		return fmt.Errorf("cluster name %q is %d bytes, more than %d: %w", name, len(name), MaxClusterNameFormat, ErrInvalid)
	}
	for i := 0; i < len(name); i++ {
		if !producerByte(name[i]) {
			return fmt.Errorf("cluster name %q has byte %q outside [A-Za-z0-9._:/-]: %w", name, name[i], ErrInvalid)
		}
	}
	return nil
}

// CheckClusterNameLimit checks that a store may write a cluster name: it has the
// format ([ValidClusterName]) and is at most [MaxClusterNameLen] bytes. Every
// error wraps [ErrInvalid].
func CheckClusterNameLimit(name string) error {
	if err := ValidClusterName(name); err != nil {
		return err
	}
	if len(name) > MaxClusterNameLen {
		return fmt.Errorf("cluster name %q is %d bytes, more than %d: %w", name, len(name), MaxClusterNameLen, ErrInvalid)
	}
	return nil
}

// AppendClusterName appends the stored form of the cluster name to dst: the byte
// 1, a uvarint length n, and the n bytes of the name. A store with no cluster name
// set (set false) stores n = 0 and the name is ignored. It does not check the name:
// the caller does ([ValidClusterName], [CheckClusterNameLimit]).
func AppendClusterName(dst []byte, name string, set bool) []byte {
	dst = append(dst, clusterNameVersion)
	if !set {
		return binary.AppendUvarint(dst, 0)
	}
	dst = binary.AppendUvarint(dst, uint64(len(name)))
	return append(dst, name...)
}

// DecodeClusterName reads a value written by [AppendClusterName], strictly. The
// bound it holds is the format's, [MaxClusterNameFormat], not the limit's, so a
// name recorded under a higher limit still reads. set is false exactly when the
// stored length is 0. It refuses an unknown leading byte, a length over the
// format's, a name with a byte outside [A-Za-z0-9._:/-], a length that is not the
// shortest uvarint, a truncated value and a trailing byte. Every error wraps
// [ErrInvalid].
func DecodeClusterName(b []byte) (name string, set bool, err error) {
	if len(b) == 0 {
		return "", false, fmt.Errorf("cluster name value is empty: %w", ErrInvalid)
	}
	if b[0] != clusterNameVersion {
		return "", false, fmt.Errorf("cluster name value has version byte %#x, want %#x: %w", b[0], clusterNameVersion, ErrInvalid)
	}
	n, w := binary.Uvarint(b[1:])
	switch {
	case w == 0:
		return "", false, fmt.Errorf("cluster name value is truncated in its length: %w", ErrInvalid)
	case w < 0:
		return "", false, fmt.Errorf("cluster name length overflows: %w", ErrInvalid)
	case w != len(binary.AppendUvarint(nil, n)):
		return "", false, fmt.Errorf("cluster name length is not the shortest uvarint: %w", ErrInvalid)
	case n > uint64(MaxClusterNameFormat):
		return "", false, fmt.Errorf("cluster name length %d is more than %d: %w", n, MaxClusterNameFormat, ErrInvalid)
	}
	rest := b[1+w:]
	switch {
	case uint64(len(rest)) < n:
		return "", false, fmt.Errorf("cluster name value is truncated: %d bytes of %d: %w", len(rest), n, ErrInvalid)
	case uint64(len(rest)) > n:
		return "", false, fmt.Errorf("cluster name value has %d trailing bytes: %w", uint64(len(rest))-n, ErrInvalid)
	case n == 0:
		return "", false, nil
	}
	name = string(rest)
	if err := ValidClusterName(name); err != nil {
		return "", false, err
	}
	return name, true, nil
}
