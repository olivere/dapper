package dapper

import (
	"testing"
)

func TestEscapeTableName(t *testing.T) {
	tests := []struct {
		Dialect       Dialect
		Input, Output string
	}{
		{MySQL, "Address", "`Address`"},
		{MySQL, "Index", "`Index`"},
		{MySQL, "With Space", "`With Space`"},
		{Sqlite3, "Address", "`Address`"},
		{Sqlite3, "Index", "`Index`"},
		{Sqlite3, "With Space", "`With Space`"},
		{PostgreSQL, "Address", `"Address"`},
		{PostgreSQL, "Index", `"Index"`},
		{PostgreSQL, "With Space", `"With Space"`},
	}

	for _, test := range tests {
		got := test.Dialect.EscapeTableName(test.Input)
		if got != test.Output {
			t.Errorf("%s: expected %v, got %v", test.Dialect, test.Output, got)
		}
	}
}

func TestEscapeColumnName(t *testing.T) {
	tests := []struct {
		Dialect       Dialect
		Input, Output string
	}{
		{MySQL, "Address", "`Address`"},
		{MySQL, "Index", "`Index`"},
		{MySQL, "With Space", "`With Space`"},
		{Sqlite3, "Address", "`Address`"},
		{Sqlite3, "Index", "`Index`"},
		{Sqlite3, "With Space", "`With Space`"},
		{PostgreSQL, "Address", `"Address"`},
		{PostgreSQL, "Index", `"Index"`},
		{PostgreSQL, "With Space", `"With Space"`},
	}

	for _, test := range tests {
		got := test.Dialect.EscapeColumnName(test.Input)
		if got != test.Output {
			t.Errorf("%s: expected %v, got %v", test.Dialect, test.Output, got)
		}
	}
}
