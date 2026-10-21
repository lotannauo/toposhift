package s3store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
)

// Names of the environment variables the credentials come from. They are read
// from the environment and from nowhere else: a secret in a flag lands in the
// process list and in shell history, and in a file it lands in backups.
const (
	EnvAccessKeyID     = "TOPOSHIFT_S3_ACCESS_KEY_ID"
	EnvSecretAccessKey = "TOPOSHIFT_S3_SECRET_ACCESS_KEY"
)

const redacted = "<redacted>"

// Secret is a credential that does not print. Every way a value usually gets
// into a log line or an error message (the fmt verbs, %#v, encoding/json,
// log/slog) yields "<redacted>"; only a conversion to string reads the value.
type Secret string

// String implements fmt.Stringer.
func (Secret) String() string { return redacted }

// Format implements fmt.Formatter, which fmt consults before anything else and
// for every verb. Without it a verb that does not print strings, such as %d,
// would print the value in its error text.
func (Secret) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, redacted) }

// GoString implements fmt.GoStringer.
func (Secret) GoString() string { return redacted }

// MarshalText implements encoding.TextMarshaler, which encoding/json and the
// text encoders use.
func (Secret) MarshalText() ([]byte, error) { return []byte(redacted), nil }

// MarshalJSON implements json.Marshaler.
func (Secret) MarshalJSON() ([]byte, error) { return json.Marshal(redacted) }

// LogValue implements slog.LogValuer.
func (Secret) LogValue() slog.Value { return slog.StringValue(redacted) }

// ErrNoCredentials is returned by [CredentialsFromEnv] when a variable is unset
// or empty.
var ErrNoCredentials = errors.New("s3store: credentials are not set")

// CredentialsFromEnv reads the access key id and the secret access key with
// getenv (os.Getenv in a program). The error names the variables that are
// missing, never a value.
func CredentialsFromEnv(getenv func(string) string) (id string, secret Secret, err error) {
	id = getenv(EnvAccessKeyID)
	secret = Secret(getenv(EnvSecretAccessKey))
	var missing []string
	if id == "" {
		missing = append(missing, EnvAccessKeyID)
	}
	if secret == "" {
		missing = append(missing, EnvSecretAccessKey)
	}
	if len(missing) > 0 {
		return "", "", fmt.Errorf("%w: set %v", ErrNoCredentials, missing)
	}
	return id, secret, nil
}
