package db

import (
	"context"
	"errors"
	"net/url"
	"os"
	"regexp"
	"testing"

	"gorm.io/gorm"
)

// testDSN returns the DSN for integration tests, skipping the test when the
// IPFSGRAM_TEST_DSN environment variable is not set. The database name from the
// DSN gets a per-package "_dbroot" suffix so this package does not collide with
// other packages' integration-test databases under `go test ./...`.
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("IPFSGRAM_TEST_DSN")
	if dsn == "" {
		t.Skip("IPFSGRAM_TEST_DSN is not set; skipping integration test")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse IPFSGRAM_TEST_DSN: %v", err)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ipfsgram_test"
	}
	u.Path += "_dbroot"
	return u.String()
}

// resetSchema drops and recreates the public schema so every test starts from
// an empty database. Migrate runs first because dbmate is what creates the
// database when it does not exist yet (fresh Postgres server).
func resetSchema(t *testing.T, dsn string) {
	t.Helper()
	if err := Migrate(dsn); err != nil {
		t.Fatalf("initial migrate: %v", err)
	}
	gdb, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer closeDB(t, gdb)
	if err := gdb.Exec(`DROP SCHEMA public CASCADE`).Error; err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if err := gdb.Exec(`CREATE SCHEMA public`).Error; err != nil {
		t.Fatalf("create schema: %v", err)
	}
}

func closeDB(t *testing.T, gdb *gorm.DB) {
	t.Helper()
	sqlDB, err := gdb.DB()
	if err != nil {
		t.Fatalf("access sql.DB: %v", err)
	}
	sqlDB.Close()
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

	gdb, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer closeDB(t, gdb)

	if err := CheckSchemaVersion(context.Background(), gdb); err != nil {
		t.Fatalf("CheckSchemaVersion after migrate: %v", err)
	}

	// Sanity-check that the migration actually created the schema.
	var n int64
	if err := gdb.Raw(`SELECT count(*) FROM config`).Scan(&n).Error; err != nil {
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

	gdb, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer closeDB(t, gdb)

	if err := gdb.Exec(`INSERT INTO schema_migrations (version) VALUES ('99999999999999')`).Error; err != nil {
		t.Fatalf("insert fake migration: %v", err)
	}

	err = CheckSchemaVersion(context.Background(), gdb)
	if !errors.Is(err, ErrSchemaAhead) {
		t.Fatalf("CheckSchemaVersion = %v, want ErrSchemaAhead", err)
	}
}

func TestCheckSchemaVersionBehind(t *testing.T) {
	dsn := testDSN(t)
	resetSchema(t, dsn)

	gdb, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer closeDB(t, gdb)

	// Empty database: schema_migrations does not exist yet.
	err = CheckSchemaVersion(context.Background(), gdb)
	if !errors.Is(err, ErrSchemaBehind) {
		t.Fatalf("CheckSchemaVersion on empty database = %v, want ErrSchemaBehind", err)
	}
}
