package dapper

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestMigrate(t *testing.T) {
	os.Remove("./migrate_test_data.db")
	db, err := sql.Open("sqlite3", "./migrate_test_data.db")
	if err != nil {
		t.Fatalf("error connection to database: %v", err)
	}
	defer db.Close()

	session := New(db).Dialect(Sqlite3)

	// Perform initial migration
	err = NewMigrator(db, Sqlite3, "./migrate_test_data/step1/").Do()
	if err != nil {
		t.Fatalf("expected migrations in step 1 to succeed, got: %v", err)
	}

	// Check that migrations table exists
	count, err := session.Count("SELECT COUNT(*) FROM sqlite_master WHERE name='"+MigrationTableName+"'", nil)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected to have '%s' table, but we don't", MigrationTableName)
	}

	// We should have 2 schema versions by now
	count, err = session.Count("SELECT COUNT(*) FROM "+MigrationTableName, nil)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected to have 2 schema entries, got: %v", count)
	}

	// Check that 'users' table exists
	count, err = session.Count("SELECT COUNT(*) FROM sqlite_master WHERE name='users'", nil)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Error("expected to have 'users' table, but we don't")
	}

	// Check that 'firms' table exists
	count, err = session.Count("SELECT COUNT(*) FROM sqlite_master WHERE name='firms'", nil)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Error("expected to have 'firms' table, but we don't")
	}

	// Upgrade with step 2
	err = NewMigrator(db, Sqlite3, "./migrate_test_data/step2/").Do()
	if err != nil {
		t.Fatalf("expected migrations in step 2 to succeed, got: %v", err)
	}

	// Now we should have 4 tables and 3 schema versions
	count, err = session.Count("SELECT COUNT(*) FROM "+MigrationTableName, nil)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected to have 3 schema entries, got: %v", count)
	}

	// Check that 'products' table exists
	count, err = session.Count("SELECT COUNT(*) FROM sqlite_master WHERE name='products'", nil)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Error("expected to have 'products' table, but we don't")
	}

	// Check that 'categories' table exists
	count, err = session.Count("SELECT COUNT(*) FROM sqlite_master WHERE name='categories'", nil)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Error("expected to have 'categories' table, but we don't")
	}

	// Upgrade with step 3, which should fail and rollback
	err = NewMigrator(db, Sqlite3, "./migrate_test_data/step3/").Do()
	if err == nil {
		t.Error("expected migrations in step 3 to fail, got no error")
	}

	// We should still have 4 tables and 3 schema versions
	count, err = session.Count("SELECT COUNT(*) FROM "+MigrationTableName, nil)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected to have 3 schema entries, got: %v", count)
	}

	// Check that 'members' table does not exist
	count, err = session.Count("SELECT COUNT(*) FROM sqlite_master WHERE name='members'", nil)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 0 {
		t.Error("expected to not have 'members' table, but we do")
	}
}
