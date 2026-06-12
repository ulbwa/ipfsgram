package db

import (
	"context"
	"errors"
	"os"
	"regexp"
	"testing"
)

// testDSN returns the DSN for integration tests, skipping the test when the
// IPFSGRAM_TEST_DSN environment variable is not set.
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("IPFSGRAM_TEST_DSN")
	if dsn == "" {
		t.Skip("IPFSGRAM_TEST_DSN is not set; skipping integration test")
	}
	return dsn
}

// resetSchema drops and recreates the public schema so every test starts
// from an empty database.
func resetSchema(t *testing.T, dsn string) {
	t.Helper()
	conn, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Exec(`DROP SCHEMA public CASCADE`); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := conn.Exec(`CREATE SCHEMA public`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
}

func TestBinarySchemaVersion(t *testing.T) {
	v := BinarySchemaVersion()
	if !regexp.MustCompile(`^\d{14}$`).MatchString(v) {
		t.Fatalf("BinarySchemaVersion() = %q, want a 14-digit timestamp", v)
	}
}

func TestMigrateAndCheckSchemaVersion(t *testing.T) {
	dsn := testDSN(t)
	resetSchema(t, dsn)

	if err := Migrate(dsn); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	conn, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	if err := CheckSchemaVersion(context.Background(), conn); err != nil {
		t.Fatalf("CheckSchemaVersion after migrate: %v", err)
	}

	// Sanity-check that the migration actually created the schema.
	var n int
	if err := conn.Get(&n, `SELECT count(*) FROM config`); err != nil {
		t.Fatalf("query config: %v", err)
	}
	if n == 0 {
		t.Fatal("config table is empty, want seeded defaults")
	}
}

func TestCheckSchemaVersionAhead(t *testing.T) {
	dsn := testDSN(t)
	resetSchema(t, dsn)

	if err := Migrate(dsn); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	conn, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Exec(`INSERT INTO schema_migrations (version) VALUES ('99999999999999')`); err != nil {
		t.Fatalf("insert fake migration: %v", err)
	}

	err = CheckSchemaVersion(context.Background(), conn)
	if !errors.Is(err, ErrSchemaAhead) {
		t.Fatalf("CheckSchemaVersion = %v, want ErrSchemaAhead", err)
	}
}

func TestCheckSchemaVersionBehind(t *testing.T) {
	dsn := testDSN(t)
	resetSchema(t, dsn)

	conn, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	// Empty database: schema_migrations does not exist yet.
	err = CheckSchemaVersion(context.Background(), conn)
	if !errors.Is(err, ErrSchemaBehind) {
		t.Fatalf("CheckSchemaVersion on empty database = %v, want ErrSchemaBehind", err)
	}
}
