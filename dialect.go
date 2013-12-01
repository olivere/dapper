package dapper

// Dialect represents SQL engine specific information.
type Dialect struct {
	TableQuoteChar       string
	ColumnQuoteChar      string
	SupportsLastInsertId bool
}

var (
	// MySQL dialect.
	MySQL = &Dialect{
		TableQuoteChar:       "",
		ColumnQuoteChar:      "`",
		SupportsLastInsertId: true,
	}

	// Sqlite3 dialect.
	Sqlite3 = &Dialect{
		TableQuoteChar:       "",
		ColumnQuoteChar:      "`",
		SupportsLastInsertId: true,
	}

	// PostgreSQL dialect.
	PostgreSQL = &Dialect{
		TableQuoteChar:       "",
		ColumnQuoteChar:      `"`,
		SupportsLastInsertId: false,
	}
)
