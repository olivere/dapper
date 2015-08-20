package dapper

import (
	"testing"
	"time"
)

type QuoteTest struct {
	dialect  Dialect
	name     string
	input    interface{}
	expected string
}

var (
	oliver       = "Oliver"
	int_1        = int(1)
	int16_1      = int(1)
	int32_1      = int(1)
	int64_1      = int(1)
	uint_1       = int(1)
	uint8_1      = int(1)
	uint16_1     = int(1)
	uint32_1     = int(1)
	uint64_1     = int(1)
	float32_0_0  = float32(0.0)
	float32_1_0  = float32(1.0)
	float32_1_5  = float32(1.5)
	float32_m1_0 = float32(-1.0)
	float32_m1_5 = float32(-1.5)
	bool_true    = true
	bool_false   = false
)

var quotetests = []QuoteTest{
	// MySQL
	{MySQL, "NULL", nil, "NULL"},
	{MySQL, "Empty string", "", "''"},
	{MySQL, "Double-quotes", "Mc'Allister", "'Mc\\'Allister'"},
	{MySQL, "ptr to string", &oliver, "'Oliver'"},
	{MySQL, "int(1)", int(1), "1"},
	{MySQL, "int16(1)", int16(1), "1"},
	{MySQL, "int32(1)", int32(1), "1"},
	{MySQL, "int64(1)", int64(1), "1"},
	{MySQL, "&int(1)", &int_1, "1"},
	{MySQL, "&int16(1)", &int16_1, "1"},
	{MySQL, "&int32(1)", &int32_1, "1"},
	{MySQL, "&int64(1)", &int64_1, "1"},
	{MySQL, "uint(1)", uint(1), "1"},
	{MySQL, "uint8(1)", uint8(1), "1"},
	{MySQL, "uint16(1)", uint16(1), "1"},
	{MySQL, "uint32(1)", uint32(1), "1"},
	{MySQL, "uint64(1)", uint64(1), "1"},
	{MySQL, "&uint(1)", &uint_1, "1"},
	{MySQL, "&uint8(1)", &uint8_1, "1"},
	{MySQL, "&uint16(1)", &uint16_1, "1"},
	{MySQL, "&uint32(1)", &uint32_1, "1"},
	{MySQL, "&uint64(1)", &uint64_1, "1"},
	{MySQL, "false", false, "0"},
	{MySQL, "true", true, "1"},
	{MySQL, "&false", &bool_false, "0"},
	{MySQL, "&true", &bool_true, "1"},
	{MySQL, "float32(0.0)", float32_0_0, "0.000000"},
	{MySQL, "float32(1.0)", float32_1_0, "1.000000"},
	{MySQL, "float32(-1.5)", float32_m1_5, "-1.500000"},
	{MySQL, "&float32(0.0)", &float32_0_0, "0.000000"},
	{MySQL, "&float32(1.0)", &float32_1_0, "1.000000"},
	{MySQL, "&float32(-1.5)", &float32_m1_5, "-1.500000"},
	// Sqlite3
	{Sqlite3, "NULL", nil, "NULL"},
	{Sqlite3, "Empty string", "", "''"},
	{Sqlite3, "Double-quotes", "Mc'Allister", "'Mc''Allister'"},
	{Sqlite3, "ptr to string", &oliver, "'Oliver'"},
	{Sqlite3, "int(1)", int(1), "1"},
	{Sqlite3, "int16(1)", int16(1), "1"},
	{Sqlite3, "int32(1)", int32(1), "1"},
	{Sqlite3, "int64(1)", int64(1), "1"},
	{Sqlite3, "&int(1)", &int_1, "1"},
	{Sqlite3, "&int16(1)", &int16_1, "1"},
	{Sqlite3, "&int32(1)", &int32_1, "1"},
	{Sqlite3, "&int64(1)", &int64_1, "1"},
	{Sqlite3, "uint(1)", uint(1), "1"},
	{Sqlite3, "uint8(1)", uint8(1), "1"},
	{Sqlite3, "uint16(1)", uint16(1), "1"},
	{Sqlite3, "uint32(1)", uint32(1), "1"},
	{Sqlite3, "uint64(1)", uint64(1), "1"},
	{Sqlite3, "&uint(1)", &uint_1, "1"},
	{Sqlite3, "&uint8(1)", &uint8_1, "1"},
	{Sqlite3, "&uint16(1)", &uint16_1, "1"},
	{Sqlite3, "&uint32(1)", &uint32_1, "1"},
	{Sqlite3, "&uint64(1)", &uint64_1, "1"},
	{Sqlite3, "false", false, "0"},
	{Sqlite3, "true", true, "1"},
	{Sqlite3, "&false", &bool_false, "0"},
	{Sqlite3, "&true", &bool_true, "1"},
	{Sqlite3, "float32(0.0)", float32_0_0, "0.000000"},
	{Sqlite3, "float32(1.0)", float32_1_0, "1.000000"},
	{Sqlite3, "float32(-1.5)", float32_m1_5, "-1.500000"},
	{Sqlite3, "&float32(0.0)", &float32_0_0, "0.000000"},
	{Sqlite3, "&float32(1.0)", &float32_1_0, "1.000000"},
	{Sqlite3, "&float32(-1.5)", &float32_m1_5, "-1.500000"},
	// PostgreSQL
	{PostgreSQL, "NULL", nil, "NULL"},
	{PostgreSQL, "Empty string", "", "''"},
	{PostgreSQL, "Double-quotes", "Mc'Allister", "'Mc\\'Allister'"},
	{PostgreSQL, "ptr to string", &oliver, "'Oliver'"},
	{PostgreSQL, "int(1)", int(1), "1"},
	{PostgreSQL, "int16(1)", int16(1), "1"},
	{PostgreSQL, "int32(1)", int32(1), "1"},
	{PostgreSQL, "int64(1)", int64(1), "1"},
	{PostgreSQL, "&int(1)", &int_1, "1"},
	{PostgreSQL, "&int16(1)", &int16_1, "1"},
	{PostgreSQL, "&int32(1)", &int32_1, "1"},
	{PostgreSQL, "&int64(1)", &int64_1, "1"},
	{PostgreSQL, "uint(1)", uint(1), "1"},
	{PostgreSQL, "uint8(1)", uint8(1), "1"},
	{PostgreSQL, "uint16(1)", uint16(1), "1"},
	{PostgreSQL, "uint32(1)", uint32(1), "1"},
	{PostgreSQL, "uint64(1)", uint64(1), "1"},
	{PostgreSQL, "&uint(1)", &uint_1, "1"},
	{PostgreSQL, "&uint8(1)", &uint8_1, "1"},
	{PostgreSQL, "&uint16(1)", &uint16_1, "1"},
	{PostgreSQL, "&uint32(1)", &uint32_1, "1"},
	{PostgreSQL, "&uint64(1)", &uint64_1, "1"},
	{PostgreSQL, "false", false, "0"},
	{PostgreSQL, "true", true, "1"},
	{PostgreSQL, "&false", &bool_false, "0"},
	{PostgreSQL, "&true", &bool_true, "1"},
	{PostgreSQL, "float32(0.0)", float32_0_0, "0.000000"},
	{PostgreSQL, "float32(1.0)", float32_1_0, "1.000000"},
	{PostgreSQL, "float32(-1.5)", float32_m1_5, "-1.500000"},
	{PostgreSQL, "&float32(0.0)", &float32_0_0, "0.000000"},
	{PostgreSQL, "&float32(1.0)", &float32_1_0, "1.000000"},
	{PostgreSQL, "&float32(-1.5)", &float32_m1_5, "-1.500000"},
}

func TestQuoting(t *testing.T) {
	for _, test := range quotetests {
		got := Quote(test.dialect, test.input)
		if got != test.expected {
			t.Errorf("%s: %s: expected %v, got %v", test.dialect, test.name, test.expected, got)
		}
	}
}

func TestQuoteTime(t *testing.T) {
	var got, expected string

	dt, _ := time.Parse("2006-01-02 15:04:05", "2013-01-24 18:14:15")

	expected = "'2013-01-24 18:14:15'"
	got = Quote(MySQL, dt)
	if got != expected {
		t.Errorf("time.Time: expected %v, got %v", expected, got)
	}

	got = Quote(MySQL, &dt)
	if got != expected {
		t.Errorf("&time.Time: expected %v, got %v", expected, got)
	}
}
