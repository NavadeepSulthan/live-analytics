// Package store is the ClickHouse adapter: connection, schema migrations, the batch writer
// used by the sink, and the query repository used by the query service.
package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Config describes how to reach ClickHouse.
type Config struct {
	Addr     string
	Database string
	User     string
	Password string
}

// Open connects to the configured database using the native protocol with LZ4 compression.
func Open(ctx context.Context, cfg Config) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:            []string{cfg.Addr},
		Auth:            clickhouse.Auth{Database: cfg.Database, Username: cfg.User, Password: cfg.Password},
		Compression:     &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    16,
		MaxIdleConns:    8,
		ConnMaxLifetime: time.Hour,
		Settings:        clickhouse.Settings{"max_execution_time": 30},
	})
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	return conn, nil
}

// Migrate creates the database if needed and applies pending migrations in filename order.
// Each file holds exactly one statement; applied versions are recorded in schema_migrations.
func Migrate(ctx context.Context, cfg Config, log *slog.Logger) error {
	bootstrap := cfg
	bootstrap.Database = "default"
	admin, err := Open(ctx, bootstrap)
	if err != nil {
		return err
	}
	// Identifiers can't be bound as parameters; cfg.Database comes from operator config, not users.
	err = admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", strings.ReplaceAll(cfg.Database, "`", "")))
	_ = admin.Close()
	if err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	conn, err := Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations
		(version String, applied_at DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY version`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[string]bool{}
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return err
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return err
		}
		applied[v] = true
	}
	rows.Close()

	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		if applied[version] {
			continue
		}
		sql, err := migrationFiles.ReadFile(name)
		if err != nil {
			return err
		}
		if err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", version, err)
		}
		if err := conn.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES (?)", version); err != nil {
			return err
		}
		log.Info("applied migration", "version", version)
	}
	return nil
}
