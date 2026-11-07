// Package migrations embeds the numbered SQL migrations of notification_service
// so the binary can apply them at startup without shipping extra files.
package migrations

import "embed"

// FS holds the migration files consumed by github.com/golang-migrate/migrate/v4
// through its iofs source driver.
//
//go:embed *.sql
var FS embed.FS
