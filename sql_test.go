package dapper

import (
	"testing"
	"time"
)

type QuoteTest struct {
	name     string
	input    interface{}
	expected string
}

var (
	oliver = "Oliver"
	int_1 = int(1)
	int16_1 = int(1)
	int32_1 = int(1)
	int64_1 = int(1)
	uint_1 = int(1)
	uint16_1 = int(1)
	uint32_1 = int(1)
	uint64_1 = int(1)
	float32_0_0 = float32(0.0)
	float32_1_0 = float32(1.0)
	float32_1_5 = float32(1.5)
	float32_m1_0 = float32(-1.0)
	float32_m1_5 = float32(-1.5)
	bool_true = true
	bool_false = false
)

var quotetests = []QuoteTest {
	{"NULL", nil, "NULL"},
	{"Empty string", "", "''"},
	{"Double-quotes", "Mc'Allister", "'Mc''Allister'"},
	{"ptr to string", &oliver, "'Oliver'"},
	{"int(1)", int(1), "1"},
	{"int16(1)", int16(1), "1"},
	{"int32(1)", int32(1), "1"},
	{"int64(1)", int64(1), "1"},
	{"&int(1)", &int_1, "1"},
	{"&int16(1)", &int16_1, "1"},
	{"&int32(1)", &int32_1, "1"},
	{"&int64(1)", &int64_1, "1"},
	{"uint(1)", uint(1), "1"},
	{"uint16(1)", uint16(1), "1"},
	{"uint32(1)", uint32(1), "1"},
	{"uint64(1)", uint64(1), "1"},
	{"&uint(1)", &uint_1, "1"},
	{"&uint16(1)", &uint16_1, "1"},
	{"&uint32(1)", &uint32_1, "1"},
	{"&uint64(1)", &uint64_1, "1"},
	{"false", false, "0"},
	{"true", true, "1"},
	{"&false", &bool_false, "0"},
	{"&true", &bool_true, "1"},
	{"float32(0.0)", float32_0_0, "0.000000"},
	{"float32(1.0)", float32_1_0, "1.000000"},
	{"float32(-1.5)", float32_m1_5, "-1.500000"},
	{"&float32(0.0)", &float32_0_0, "0.000000"},
	{"&float32(1.0)", &float32_1_0, "1.000000"},
	{"&float32(-1.5)", &float32_m1_5, "-1.500000"},
}

func TestQuoting(t *testing.T) {
	for _, test := range quotetests {
		got := Quote(test.input)
		if got != test.expected {
			t.Errorf("%s: expected %v, got %v", test.name, test.expected, got)
		}
	}
}

func TestQuoteTime(t *testing.T) {
	var got, expected string

	dt, _ := time.Parse("2006-01-02 15:04:05", "2013-01-24 18:14:15")

	expected = "'2013-01-24 18:14:15'"
	got = Quote(dt)
	if got != expected {
		t.Errorf("time.Time: expected %v, got %v", expected, got)
	}
	
	got = Quote(&dt)
	if got != expected {
		t.Errorf("&time.Time: expected %v, got %v", expected, got)
	}
}

