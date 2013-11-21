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
	case int, int16, int32, int64, uint, uint16, uint32, uint64:
		return fmt.Sprintf("%d", data)
	case *int:
		if data != nil {
			v := val.(*int)
			return fmt.Sprintf("%d", *v)
		}
		return "NULL"
	case *int16:
		if data != nil {
			v := val.(*int16)
			return fmt.Sprintf("%d", *v)
		}
		return "NULL"
	case *int32:
		if data != nil {
			v := val.(*int32)
			return fmt.Sprintf("%d", *v)
		}
		return "NULL"
	case *int64:
		if data != nil {
			v := val.(*int64)
			return fmt.Sprintf("%d", *v)
		}
		return "NULL"
	case *uint16:
		if data != nil {
			v := val.(*uint16)
			return fmt.Sprintf("%d", *v)
		}
		return "NULL"
	case *uint32:
		if data != nil {
			v := val.(*uint32)
			return fmt.Sprintf("%d", *v)
		}
		return "NULL"
	case *uint64:
		if data != nil {
			v := val.(*uint64)
			return fmt.Sprintf("%d", *v)
		}
		return "NULL"
	case float32, float64:
		return fmt.Sprintf("%f", data)
	case *float32:
		if data != nil {
			v := val.(*float32)
			return fmt.Sprintf("%f", *v)
		}
		return "NULL"
	case *float64:
		if data != nil {
			v := val.(*float64)
			return fmt.Sprintf("%f", *v)
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
		return fmt.Sprintf("'%s'", QuoteString(data.Format("2006-01-02 15:04:05")))
	case *time.Time:
		if data != nil {
			t := val.(*time.Time)
			return fmt.Sprintf("'%s'", QuoteString((*t).Format("2006-01-02 15:04:05")))
		}
		return "NULL"
	}
	panic(fmt.Sprintf("SQL quoting for type %s is not supported", reflect.TypeOf(val)))
	return ""
}

func QuoteString(s string) string {
	q := reBackslash.ReplaceAllString(s, "\\\\")
	return reSingleQuote.ReplaceAllString(q, "\\'")
}

func EscapeColumnName(s string) string {
	return fmt.Sprintf("`%s`", s)
}

func EscapeTableName(s string) string {
	return s
}
