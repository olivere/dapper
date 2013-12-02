package dapper

import (
	"fmt"
)

// Dialect represents SQL engine specific information.
type Dialect interface {
	EscapeTableName(string) string
	EscapeColumnName(string) string
	SupportsLastInsertId() bool
	GetCreateMigrationTableSQL(string) string
	InsertMigrationTableVersionSQL(string) string
}

// -- MySQL --

type MySQLDialect struct{}

func (mysql *MySQLDialect) String() string {
	return "MySQLDialect"
}

func (mysql *MySQLDialect) EscapeTableName(tableName string) string {
	return fmt.Sprintf("`%s`", tableName)
}

func (mysql *MySQLDialect) EscapeColumnName(columnName string) string {
	return fmt.Sprintf("`%s`", columnName)
}

func (mysql *MySQLDialect) SupportsLastInsertId() bool {
	return true
}

func (mysql *MySQLDialect) GetCreateMigrationTableSQL(tableName string) string {
	return `
CREATE TABLE IF NOT EXISTS ` + mysql.EscapeTableName(tableName) + ` (
  version integer not null primary key,
  created datetime not null
)`
}

func (mysql *MySQLDialect) InsertMigrationTableVersionSQL(tableName string) string {
	return `
INSERT INTO ` + mysql.EscapeTableName(tableName) + ` (version,created) VALUES (?, NOW())
    ON DUPLICATE KEY UPDATE created=NOW()
`
}

// -- Sqlite3 --

type Sqlite3Dialect struct{}

func (sqlite3 *Sqlite3Dialect) String() string {
	return "Sqlite3Dialect"
}

func (sqlite3 *Sqlite3Dialect) EscapeTableName(tableName string) string {
	return fmt.Sprintf("`%s`", tableName)
}

func (sqlite3 *Sqlite3Dialect) EscapeColumnName(columnName string) string {
	return fmt.Sprintf("`%s`", columnName)
}

func (sqlite3 *Sqlite3Dialect) SupportsLastInsertId() bool {
	return true
}

func (sqlite3 *Sqlite3Dialect) GetCreateMigrationTableSQL(tableName string) string {
	return `
CREATE TABLE IF NOT EXISTS ` + sqlite3.EscapeTableName(tableName) + ` (
  version integer not null primary key,
  created datetime not null
)`
}

func (sqlite3 *Sqlite3Dialect) InsertMigrationTableVersionSQL(tableName string) string {
	return `
INSERT OR IGNORE INTO ` + sqlite3.EscapeTableName(tableName) + ` (version,created) VALUES (?, date('now'))
`
}

// -- PostgreSQL --

type PostgreSQLDialect struct{}

func (psql *PostgreSQLDialect) String() string {
	return "PostgreSQLDialect"
}

func (psql *PostgreSQLDialect) EscapeTableName(tableName string) string {
	return fmt.Sprintf(`"%s"`, tableName)
}

func (psql *PostgreSQLDialect) EscapeColumnName(columnName string) string {
	return fmt.Sprintf(`"%s"`, columnName)
}

func (psql *PostgreSQLDialect) SupportsLastInsertId() bool {
	return false
}

func (psql *PostgreSQLDialect) GetCreateMigrationTableSQL(tableName string) string {
	return `
CREATE TABLE IF NOT EXISTS ` + psql.EscapeTableName(tableName) + ` (
  version integer not null primary key,
  created datetime not null
)`
}

func (psql *PostgreSQLDialect) InsertMigrationTableVersionSQL(tableName string) string {
	return `
INSERT INTO ` + psql.EscapeTableName(tableName) + ` (version,created) VALUES ($1, CURRENT_TIMESTAMP)
`
}

var (
	// MySQL dialect.
	MySQL = &MySQLDialect{}

	// Sqlite3 dialect.
	Sqlite3 = &Sqlite3Dialect{}

	// PostgreSQL dialect.
	PostgreSQL = &PostgreSQLDialect{}
)
