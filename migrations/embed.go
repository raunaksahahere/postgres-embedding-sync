// Package migrations embeds the versioned SQL that `embedsync init` applies.
//
// Files are Go text/templates so identifiers from the YAML config (schema,
// publication and role names) are substituted after validation against a
// strict identifier pattern.
package migrations

import "embed"

// FS holds source/*.sql and sink/*.sql.
//
//go:embed source/*.sql sink/*.sql
var FS embed.FS
