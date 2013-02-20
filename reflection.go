package dapper

import (
	"reflect"
	"strings"
)

var (
	// Caches information about types
	typeCache map[reflect.Type]*typeInfo
)

func init() {
	typeCache = make(map[reflect.Type]*typeInfo)
}

type typeInfo struct {
	// The type in Go
	Type reflect.Type
	// Table name
	TableName string
	// Names of the type in Go
	FieldNames []string
	// Detailed information indexed by field name
	FieldInfos map[string]*fieldInfo
	// Names of the database columns
	ColumnNames []string
	// Detailed information indexed by column name
	ColumnInfos map[string]*fieldInfo
}

// Information about database mapping.
type fieldInfo struct {
	// Name of the type in Go
	FieldName string
	// Name of the database column
	ColumnName string
	// Type of the field in Go (int32, string etc.)
	Type reflect.Type
	// Is this field specified as primarykey (... `dapper:"id,primarykey"`)
	IsPrimaryKey bool
	// Is this field specified as auto-increment (... `dapper:"id,autoincrement"`)
	IsAutoIncrement bool
	// Is this field specified as transient (... `dapper:"-"`)
	IsTransient bool
}

// Adds information about a specific type to the type cache.
func AddType(gotype reflect.Type) (*typeInfo, error) {
	if ti, found := typeCache[gotype]; found {
		return ti, nil
	}

	ti := &typeInfo{
		Type:       gotype,
		TableName:  "",
		FieldNames: make([]string, 0),
		FieldInfos: make(map[string]*fieldInfo),
		ColumnNames: make([]string, 0),
		ColumnInfos: make(map[string]*fieldInfo),
	}

	// Grab information about all the fields
	n := gotype.NumField()
	for i := 0; i < n; i++ {
		field := gotype.Field(i)

		fi := &fieldInfo{
			FieldName:       field.Name,
			Type:            field.Type,
			IsPrimaryKey:    false,
			IsAutoIncrement: false,
			IsTransient:     false,
		}

		// Additional information about this type are attached
		// to the "dapper" tag
		tag := field.Tag.Get("dapper")
		if tag != "" {
			tags := strings.Split(tag, ",")
			if len(tags) >= 1 {
				// "-" means: ignore this field/column (transient)
				if tags[0] != "-" {
					fi.ColumnName = tags[0]
				} else {
					fi.ColumnName = ""
					fi.IsTransient = true
				}

				// Check for additional tags
				for _, t := range tags[1:] {
					if t == "primarykey" || t == "pk" {
						fi.IsPrimaryKey = true
					}
					if t == "autoincrement" || t == "serial" {
						fi.IsAutoIncrement = true
					}
					if strings.HasPrefix(t, "table") {
						// table=xxx
						tableAndName := strings.SplitN(t, "=", 2)
						ti.TableName = tableAndName[1]
					}
				}
			}
		} else {
			// No `dapper` tag, so treat field name as column name
			fi.ColumnName = field.Name
		}

		ti.FieldNames = append(ti.FieldNames, fi.FieldName)
		ti.FieldInfos[fi.FieldName] = fi

		if !fi.IsTransient {
			ti.ColumnNames = append(ti.ColumnNames, fi.ColumnName)
			ti.ColumnInfos[fi.ColumnName] = fi
		}

		typeCache[gotype] = ti
	}

	return ti, nil
}

// HasAutoIncrement returns information about the autoincrement field
// of the specified type.
func (ti *typeInfo) HasAutoIncrement() (*fieldInfo, bool) {
	for _, fi := range ti.FieldInfos {
		if fi.IsAutoIncrement {
			return fi, true
		}
	}
	return nil, false
}

// HasPrimaryKey returns information about the primary key field
// of the specified type.
func (ti *typeInfo) HasPrimaryKey() (*fieldInfo, bool) {
	for _, fi := range ti.FieldInfos {
		if fi.IsPrimaryKey {
			return fi, true
		}
	}
	return nil, false
}
