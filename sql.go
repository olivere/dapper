package dapper

import (
	"fmt"
	"reflect"
	"regexp"
	"time"
)

var (
	reBackslash   = regexp.MustCompile(`(\\)`)
	reSingleQuote = regexp.MustCompile("'")
)

func Quote(val interface{}) string {
	switch data := val.(type) {
	case nil:
		return "NULL"
	case string:
		return fmt.Sprintf("'%s'", QuoteString(data))
	case *string:
		if data != nil {
			return fmt.Sprintf("'%s'", QuoteString(*data))
		}
		return "NULL"
	case int, int16, int32, int64, uint16, uint32, uint64:
		return fmt.Sprintf("%d", data)
	case *int, *int16, *int32, *int64, *uint16, *uint32, *uint64:
		if data != nil {
			return fmt.Sprintf("%d", data)
		}
		return "NULL"
	case float32, float64:
		return fmt.Sprintf("%f", data)
	case *float32, *float64:
		if data != nil {
			return fmt.Sprintf("%f", data)
		}
		return "NULL"
	case bool:
		if data {
			return "1"
		}
		return "0"
	case *bool:
		if data != nil {
			if *data {
				return "1"
			}
			return "0"
		}
		return "NULL"
	case time.Time:
		panic("SQL quoting for type time.Time is not yet supported")
	}
	panic(fmt.Sprintf("SQL quoting for type %s is not supported", reflect.TypeOf(val)))
	return ""
}

func QuoteString(s string) string {
	q := reBackslash.ReplaceAllString(s, "\\\\")
	return reSingleQuote.ReplaceAllString(q, "''")
}
