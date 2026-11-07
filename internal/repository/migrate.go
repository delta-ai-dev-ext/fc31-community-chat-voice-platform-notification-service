package repository

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"notification_service/migrations"
)

// Migrate applies every embedded migration to the database identified by
// databaseURL. It is safe to call on every replica start: already-applied
// migrations are skipped by golang-migrate.
func Migrate(databaseURL string) error {
	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("repository: build migration source: %w", err)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("repository: open migration connection: %w", err)
	}
	defer db.Close()

	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		return fmt.Errorf("repository: build migration driver: %w", err)
	}
	runner, err := migrate.NewWithInstance("iofs", source, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("repository: build migrator: %w", err)
	}
	if err := runner.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("repository: apply migrations: %w", err)
	}
	return nil
}

// Ensure the pool transactor satisfies the documented contract.
var _ Transactor = (*PoolTransactor)(nil)

var (
	_ NotificationRepository   = (*NotificationStore)(nil)
	_ PreferenceRepository     = (*PreferenceStore)(nil)
	_ RecipientRepository      = (*RecipientStore)(nil)
	_ EmailDeliveryRepository  = (*EmailDeliveryStore)(nil)
	_ ProcessedEventRepository = (*ProcessedEventStore)(nil)
	_ MentionWindowRepository  = (*MentionWindowStore)(nil)
	_ OutboxRepository         = (*OutboxStore)(nil)
)
