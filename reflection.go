package dapper

import (
	"errors"
	"fmt"
	_ "log"
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

// typeInfo contains all dapper-specific information about a type.
// These kind of information are specified via dapper-tags in the struct.
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
	// Names of the columns containing associations
	AssocFieldNames []string
	// 1:1 associations
	OneToOneInfos map[string]*oneToOneInfo
	// 1:n associations
	OneToManyInfos map[string]*oneToManyInfo
}

// fieldInfo contains DB mapping information about
// a single property of a struct.
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

// oneToOneInfo contains information about a 1:1 reference to another table.
type oneToOneInfo struct {
	// Name of the type in Go
	FieldName string
	// SelfType is the type of the field in Go (e.g. *OrderItem)
	SelfType reflect.Type
	// TargetType is the type of the referenced struct in Go (e.g. *Order)
	TargetType reflect.Type
	// ForeignKeyField contains the name of the field to be used as foreign key
	ForeignKeyField string
}

// oneToManyInfo contains information about a 1:n reference to another table.
type oneToManyInfo struct {
	// Name of the type in Go
	FieldName string
	// SliceType of the field in Go (e.g. []*OrderItem)
	SliceType reflect.Type
	// ElemType of the field in Go (e.g. *OrderItem)
	ElemType reflect.Type
	// ForeignKeyField contains the name of the field to be used as foreign key
	ForeignKeyField string
}

// Adds information about a specific type to the type cache.
func AddType(gotype reflect.Type) (*typeInfo, error) {
	// Always redirect to the base type, i.e. if type *Order or
	// []*Order is tries to be added, it is refered back to type Order
	for {
		kind := gotype.Kind()
		if kind == reflect.Array || kind == reflect.Ptr || kind == reflect.Slice {
			gotype = gotype.Elem()
		} else {
			break
		}
	}

	// Find the type in the cache
	if ti, found := typeCache[gotype]; found {
		return ti, nil
	}

	// Inspect and add to type cache
	ti := &typeInfo{
		Type:            gotype,
		TableName:       "",
		FieldNames:      make([]string, 0),
		FieldInfos:      make(map[string]*fieldInfo),
		ColumnNames:     make([]string, 0),
		ColumnInfos:     make(map[string]*fieldInfo),
		AssocFieldNames: make([]string, 0),
		OneToOneInfos:   make(map[string]*oneToOneInfo),
		OneToManyInfos:  make(map[string]*oneToManyInfo),
	}

	// Grab information about all the fields
	n := gotype.NumField()
	for i := 0; i < n; i++ {
		field := gotype.Field(i)

		// Only support certain types of fields
		switch field.Type.Kind() {
		case reflect.Chan,
			reflect.Func,
			reflect.Interface,
			reflect.Map,
			//reflect.Slice,
			//reflect.Struct,
			reflect.UnsafePointer:
			continue
		}

		fi := &fieldInfo{
			FieldName:       field.Name,
			Type:            field.Type,
			IsPrimaryKey:    false,
			IsAutoIncrement: false,
			IsTransient:     false,
		}

		var oneToOne *oneToOneInfo
		var oneToMany *oneToManyInfo

		// Additional information about this type are attached
		// to the "dapper" tag
		tag := field.Tag.Get("dapper")
		if tag != "" {
			//log.Printf("got tag %s", tag)
			// Check for associations
			if strings.HasPrefix(tag, "oneToMany") {
				// oneToMany=<foreign-key-field-name>
				parts := strings.SplitN(tag, "=", 2)
				if len(parts) != 2 {
					return nil, errors.New(fmt.Sprintf("invalid oneToMany specification for field %s: %s", field.Name, tag))
				}
				oneToMany = &oneToManyInfo{
					FieldName:       field.Name,
					SliceType:       field.Type,
					ElemType:        field.Type.Elem(),
					ForeignKeyField: parts[1],
				}
				fi = nil
			} else if strings.HasPrefix(tag, "oneToOne") {
				// oneToOne=<foreign-key-field-name>
				parts := strings.SplitN(tag, "=", 2)
				if len(parts) != 2 {
					return nil, errors.New(fmt.Sprintf("invalid oneToOne specification for field %s: %s", field.Name, tag))
				}
				oneToOne = &oneToOneInfo{
					FieldName:       field.Name,
					SelfType:        gotype,
					TargetType:      field.Type,
					ForeignKeyField: parts[1],
				}
				fi = nil
			} else {
				// Field name
				tags := strings.Split(tag, ",")
				if len(tags) >= 1 {
					if tags[0] == "-" {
						// Ignore this field/column (transient)
						fi.ColumnName = ""
						fi.IsTransient = true
					} else {
						// Normal column
						fi.ColumnName = tags[0]
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
			} // end of field name
		} else {
			// No `dapper` tag, so treat field name as column name
			fi.ColumnName = field.Name
		}

		if fi != nil {
			// we have a field
			ti.FieldNames = append(ti.FieldNames, fi.FieldName)
			ti.FieldInfos[fi.FieldName] = fi

			if !fi.IsTransient {
				ti.ColumnNames = append(ti.ColumnNames, fi.ColumnName)
				ti.ColumnInfos[fi.ColumnName] = fi
			}
		}

		if oneToOne != nil {
			// we have a 1:1 association
			ti.AssocFieldNames = append(ti.AssocFieldNames, oneToOne.FieldName)
			ti.OneToOneInfos[oneToOne.FieldName] = oneToOne
		}

		if oneToMany != nil {
			// we have a 1:n association
			ti.AssocFieldNames = append(ti.AssocFieldNames, oneToMany.FieldName)
			ti.OneToManyInfos[oneToMany.FieldName] = oneToMany
		}

		typeCache[gotype] = ti
	}

	return ti, nil
}

// GetAutoIncrement returns information about the autoincrement field
// of the specified type.
func (ti *typeInfo) GetAutoIncrement() (*fieldInfo, bool) {
	for _, fi := range ti.FieldInfos {
		if fi.IsAutoIncrement {
			return fi, true
		}
	}
	return nil, false
}

// GetPrimaryKey returns information about the primary key field
// of the specified type.
func (ti *typeInfo) GetPrimaryKey() (*fieldInfo, bool) {
	for _, fi := range ti.FieldInfos {
		if fi.IsPrimaryKey {
			return fi, true
		}
	}
	return nil, false
}

// GetTableName returns the name of the table
// referenced via the association.
func (info *oneToOneInfo) GetTableName() (string, error) {
	// Get type information for the referenced type
	ti, err := AddType(info.TargetType)
	if err != nil {
		return "", err
	}
	return ti.TableName, nil
}

// GetColumnName returns the column name of the table
// referenced via the association.
func (info *oneToOneInfo) GetColumnName() (string, error) {
	// Get type information for self
	ti, err := AddType(info.TargetType)
	if err != nil {
		return "", err
	}

	pk, found := ti.GetPrimaryKey()
	if !found {
		return "", ErrNoPrimaryKey
	}

	return pk.ColumnName, nil
}

// GetTableName returns the name of the table
// referenced via the association.
func (info *oneToManyInfo) GetTableName() (string, error) {
	ti, err := AddType(info.ElemType)
	if err != nil {
		return "", err
	}
	return ti.TableName, nil
}

// GetColumnName returns the column name of the table
// referenced via the association.
func (info *oneToManyInfo) GetColumnName() (string, error) {
	ti, err := AddType(info.ElemType)
	if err != nil {
		return "", err
	}

	// Iterate through all fields of the referenced type
	// and find the foreign key field
	for fieldName, fi := range ti.FieldInfos {
		if fieldName == info.ForeignKeyField {
			// Found, so return its column name
			return fi.ColumnName, nil
		}
	}

	// Foreign key not found
	return "", errors.New(fmt.Sprintf("dapper: no column found for field %s in table %s", info.ForeignKeyField, ti.TableName))
}
