package storage

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

// NewTursoDB opens a connection to the Turso LibSQL database via database/sql
func NewTursoDB(dbURL, authToken string) (*sql.DB, error) {
	if dbURL == "" {
		return nil, fmt.Errorf("dbURL tidak boleh kosong")
	}

	dsn := dbURL
	driverName := "libsql"

	// If the URL uses a local file or in-memory for testing / dev
	if strings.HasPrefix(dbURL, "file:") || dbURL == ":memory:" || strings.HasPrefix(dbURL, "sqlite:") {
		driverName = "sqlite"
		if strings.HasPrefix(dbURL, "sqlite:") {
			dsn = strings.TrimPrefix(dbURL, "sqlite:")
		}
	} else {
		// Append authToken to the URL if provided and not already present in the URL
		if authToken != "" && !strings.Contains(dsn, "authToken=") {
			separator := "?"
			if strings.Contains(dsn, "?") {
				separator = "&"
			}
			dsn = fmt.Sprintf("%s%sauthToken=%s", dsn, separator, url.QueryEscape(authToken))
		}
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("gagal membuka koneksi database (%s): %w", driverName, err)
	}

	// Configure connection pooling
	db.SetMaxOpenConns(15)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(1 * time.Hour)

	return db, nil
}
