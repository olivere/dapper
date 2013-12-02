package dapper

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var (
	// reMigrationName specifies the regular expression that migration
	// names must fulfill, i.e. a number at the beginning and a .sql extension
	// at the end.
	reMigrationName = regexp.MustCompile("(?:([0-9]+).*\\.sql$)")
)

const (
	// MigrationTableName is the name of the migrations database table.
	MigrationTableName = "dapper_migrations"
)

// migration is a single update unit.
type migration struct {
	Version int    // Version number (monotonically increasing)
	Path    string // Path is the file name of the migration
}

func (m migration) String() string {
	return fmt.Sprintf("Path=%s,Version=%d", m.Path, m.Version)
}

type migrator struct {
	db      *sql.DB
	path    string
	dialect Dialect
	verbose bool
	debug   bool
	out     io.Writer
}

func NewMigrator(db *sql.DB, path string) *migrator {
	return &migrator{db: db, path: path, out: os.Stdout}
}

func (m *migrator) Dialect(dialect Dialect) *migrator {
	m.dialect = dialect
	return m
}

func (m *migrator) Verbose(verbose bool) *migrator {
	m.verbose = verbose
	return m
}

func (m *migrator) Debug(debug bool) *migrator {
	m.debug = debug
	return m
}

func (m *migrator) Out(out io.Writer) *migrator {
	m.out = out
	return m
}

func (m *migrator) Do() error {
	m.printf("Reading migrations from %s\n", m.path)

	// Create migration table (unless it already exists)
	_, err := m.db.Exec(m.dialect.GetCreateMigrationTableSQL(MigrationTableName))
	if err != nil {
		return err
	}

	// Determine current migration number
	var versionN sql.NullInt64
	err = m.db.QueryRow(`SELECT version FROM ` + MigrationTableName + ` ORDER BY version DESC LIMIT 1`).Scan(&versionN)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	version := int(-1)
	if versionN.Valid {
		version = int(versionN.Int64)
	}
	if version >= 0 {
		m.printf("Schema version: %d\n", version)
	} else {
		m.printf("No schema version found\n")
	}

	// Retrieve the list of all migrations in the given path
	migrations := make([]migration, 0)
	scripts, err := filepath.Glob(path.Join(m.path, "*.sql"))
	if err != nil {
		return err
	}
	for _, script := range scripts {
		matches := reMigrationName.FindStringSubmatch(filepath.Base(script))
		if len(matches) == 2 {
			scriptVersion, _ := strconv.Atoi(matches[1])
			migration := migration{Version: scriptVersion, Path: script}
			migrations = append(migrations, migration)
		}
	}

	// Apply or skip all migrations
	for _, migration := range migrations {
		if migration.Version > version {
			m.printf("Applying %s\n", filepath.Base(migration.Path))

			// Read file
			data, err := ioutil.ReadFile(migration.Path)
			if err != nil {
				return err
			}
			m.debugf(string(data))
			lines := strings.Split(string(data), ";")

			// Begin transaction
			tx, err := m.db.Begin()
			if err != nil {
				return err
			}

			// Execute SQL script
			for _, line := range lines {
				line = strings.TrimSpace(line)

				// Split lines and remove comments
				var sqlbuf bytes.Buffer
				for _, line := range strings.Split(line, "\n") {
					if !strings.HasPrefix(line, "--") && !strings.HasPrefix(line, "#") {
						sqlbuf.WriteString(line)
						sqlbuf.WriteString("\n")
					}
				}
				sql := strings.TrimSpace(sqlbuf.String())
				if sql != "" {
					m.debugf("%s\n", sql)

					_, err := tx.Exec(sql)
					if err != nil {
						tx.Rollback()
						return err
					}
				}
			}

			// Update to new version
			sql := m.dialect.InsertMigrationTableVersionSQL(MigrationTableName)
			_, err = tx.Exec(sql, migration.Version)
			if err != nil {
				tx.Rollback()
				return err
			}

			// Commit
			if err := tx.Commit(); err != nil {
				return err
			}

			version = migration.Version
		} else {
			m.printf("Skipping %s\n", filepath.Base(migration.Path))
		}
	}

	return nil
}

func (m *migrator) printf(format string, args ...interface{}) {
	if m.verbose && m.out != nil {
		fmt.Fprintf(m.out, format, args...)
	}
}

func (m *migrator) debugf(format string, args ...interface{}) {
	if m.debug && m.out != nil {
		fmt.Fprintf(m.out, format, args...)
	}
}
