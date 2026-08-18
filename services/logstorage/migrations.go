package main

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func RunMigrations(ctx context.Context, dsn string) error {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("failed opening database connection: %w", err)
	}
	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed pinging database: %w", err)
	}

	goose.SetBaseFS(migrationsFS)
	err = goose.SetDialect("clickhouse")
	if err != nil {
		return fmt.Errorf("failed setting clickhouse dialect: %w", err)
	}
	err = goose.RunContext(ctx, "up", db, "migrations")
	if err != nil {
		return fmt.Errorf("failed running migrations: %w", err)
	}

	return nil
}
