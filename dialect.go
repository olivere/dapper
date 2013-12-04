package dapper

import (
	"bytes"
	"fmt"
	"regexp"
)

const MaxInt = int(^uint(0) >> 1)

// Dialect represents SQL engine specific information.
type Dialect interface {
	QuoteString(string) string
	EscapeTableName(string) string
	EscapeColumnName(string) string
	SupportsLastInsertId() bool
	GetLimitString(query string, skip, take int) string
	GetCreateMigrationTableSQL(string) string
	InsertMigrationTableVersionSQL(string) string
}

var (
	reBackslash   = regexp.MustCompile(`(\\)`)
	reSingleQuote = regexp.MustCompile("'")
)

// -- MySQL --

type MySQLDialect struct{}

func (mysql *MySQLDialect) String() string {
	return "MySQLDialect"
}

func (mysql *MySQLDialect) QuoteString(s string) string {
	q := reBackslash.ReplaceAllString(s, "\\\\")
	return reSingleQuote.ReplaceAllString(q, "\\'")
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

func (mysql *MySQLDialect) GetLimitString(query string, skip, take int) string {
	if take < 0 && skip < 0 {
		return query
	}
	var b bytes.Buffer
	b.WriteString(query)
	b.WriteString(" LIMIT ")
	if skip > 0 {
		b.WriteString(fmt.Sprintf("%d", skip))
		if take >= 0 {
			b.WriteString(",")
			b.WriteString(fmt.Sprintf("%d", take))
		}
	} else {
		b.WriteString(fmt.Sprintf("%d", take))
	}
	return b.String()
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

func (sqlite3 *Sqlite3Dialect) QuoteString(s string) string {
	q := reBackslash.ReplaceAllString(s, "\\\\")
	return reSingleQuote.ReplaceAllString(q, "''")
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

func (sqlite3 *Sqlite3Dialect) GetLimitString(query string, skip, take int) string {
	if take < 0 && skip < 0 {
		return query
	}
	var b bytes.Buffer
	b.WriteString(query)
	b.WriteString(" LIMIT ")
	if take > 0 {
		b.WriteString(fmt.Sprintf("%d", take))
	} else {
		// We must have a limit
		b.WriteString(fmt.Sprintf("%d", MaxInt))
	}
	if skip > 0 {
		b.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}
	return b.String()
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

func (psql *PostgreSQLDialect) QuoteString(s string) string {
	q := reBackslash.ReplaceAllString(s, "\\\\")
	return reSingleQuote.ReplaceAllString(q, "\\'")
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

func (psql *PostgreSQLDialect) GetLimitString(query string, skip, take int) string {
	if take < 0 && skip < 0 {
		return query
	}
	var b bytes.Buffer
	b.WriteString(query)
	if take > 0 {
		b.WriteString(fmt.Sprintf(" LIMIT %d", take))
	}
	if skip > 0 {
		b.WriteString(fmt.Sprintf(" OFFSET %d", skip))
	}
	return b.String()
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
