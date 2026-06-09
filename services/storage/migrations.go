package main

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func RunMigrations(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("failed opening database connection: %w", err)
	}
	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed pinging database: %w", err)
	}

	goose.SetBaseFS(migrationsFS)
	err = goose.SetDialect("postgres")
	if err != nil {
		return fmt.Errorf("failed setting postgres dialect: %w", err)
	}
	err = goose.RunContext(ctx, "up", db, "migrations")
	if err != nil {
		return fmt.Errorf("failed running migrations: %w", err)
	}

	return nil
}
