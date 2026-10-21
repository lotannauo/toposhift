// Package scripts holds the files the DuckDB baseline is run from, so that a binary
// carries the exact bytes whose digest every output records.
package scripts

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
)

// SettingsSQL is settings.sql, the one source of every DuckDB setting.
//
//go:embed settings.sql
var SettingsSQL []byte

// SettingsSHA256 is the SHA-256 of settings.sql as lowercase hex.
func SettingsSHA256() string {
	sum := sha256.Sum256(SettingsSQL)
	return hex.EncodeToString(sum[:])
}
