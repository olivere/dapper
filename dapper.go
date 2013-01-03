package dapper

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	ErrWrongType = errors.New("dapper: wrong type")
)

// Returns the first result of the specified SQL query in dst.
// Parameters in sql start with a colon and will be substituted by the
// corresponding field in the param object. If there are no substitutions,
// pass nil as param.
//
// Notice that sql.ErrNoRows is returned (just as with sql.QueryRow) when
// there is no row found.
// 
// Example:
// param := UserByIdQuery{Id: 42}
// result := User{}
// err := dapper.First(db, "select * from users where id=:Id", param, &result)
func First(db *sql.DB, sqlQuery string, param interface{}, dst interface{}) error {
	// Get information about dst
	dstValue := reflect.ValueOf(dst)
	if dstValue.IsNil() {
		return errors.New("dst is nil")
	}

	if dstValue.Kind() != reflect.Ptr {
		return errors.New("dst must be a pointer to a struct")
	}

	indirectValue := reflect.Indirect(dstValue)
	gotype := indirectValue.Type()

	dstInfo, err := AddType(gotype)
	if err != nil {
		return err
	}

	// Get information about param
	if param != nil {
		paramValue := reflect.ValueOf(param)
		paramInfo, err := AddType(paramValue.Type())
		if err != nil {
			return err
		}

		// Substitute parameters in SQL statement
		for paramName, _ := range paramInfo.FieldInfos {
			// Get value of field in param
			field := paramValue.FieldByName(paramName)
			// TODO check for nil and invalid field
			value := field.Interface()
			quoted := Quote(value)
			sqlQuery = strings.Replace(sqlQuery, ":"+paramName, quoted, -1)
		}
	}

	// We use Query instead of QueryRow, because row does not contain Column information
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return err
	}

	// Scan fills all fields in dst here
	if rows.Next() {
		dstFields := make([]interface{}, 0)
		dbColumnNames, err := rows.Columns()
		if err != nil {
			return err
		}
		for _, dbColName := range dbColumnNames {
			fi, found := dstInfo.ColumnInfos[dbColName]
			if !found {
				return errors.New(
					fmt.Sprintf("type %s: found no corresponding mapping "+
						"for column %s in result", gotype, dbColName))
			}

			field := dstValue.Elem().FieldByName(fi.FieldName)
			dstFields = append(dstFields, field.Addr().Interface())
		}

		// Scan results
		err = rows.Scan(dstFields...)
		if err != nil {
			return err
		}
	} else {
		// If there's no row, we should return sql.ErrNoRows
		return sql.ErrNoRows
	}

	return nil
}

// Returns a list of results of a specified SQL query in dst.
// Parameters in sql start with a colon and will be substituted by the
// corresponding field in the param object. If there are no substitutions,
// pass nil as param.
//
// Example:
// param := UserByCompanyQuery{CompanyId: 42}
// results, err := dapper.Query(db, "select * from users where company_id=:CompanyId oder by email limit 10", param, reflect.TypeOf(User{}))
func Query(db *sql.DB, sql string, param interface{}, gotype reflect.Type) ([]interface{}, error) {
	dstInfo, err := AddType(gotype)
	if err != nil {
		return nil, err
	}

	// Get information about param
	if param != nil {
		paramValue := reflect.ValueOf(param)
		paramInfo, err := AddType(paramValue.Type())
		if err != nil {
			return nil, err
		}

		// Substitute parameters in SQL statement
		for paramName, _ := range paramInfo.FieldInfos {
			// Get value of field in param
			field := paramValue.FieldByName(paramName)
			// TODO check for nil and invalid field
			value := field.Interface()
			quoted := Quote(value)
			sql = strings.Replace(sql, ":"+paramName, quoted, -1)
		}
	}

	rows, err := db.Query(sql)
	if err != nil {
		return nil, err
	}

	// Results
	results := make([]interface{}, 0)

	for rows.Next() {
		// Prepare destination fields for Scan
		singleResult := reflect.New(gotype)

		dstFields := make([]interface{}, 0)
		dbColumnNames, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		for _, dbColName := range dbColumnNames {
			fi, found := dstInfo.ColumnInfos[dbColName]
			if !found {
				return nil, errors.New(
					fmt.Sprintf("type %s: found no corresponding mapping "+
						"for column %s in result", gotype, dbColName))
			}

			field := singleResult.Elem().FieldByName(fi.FieldName)
			dstFields = append(dstFields, field.Addr().Interface())
		}


		// Scan fills all fields in dst here
		err = rows.Scan(dstFields...)
		if err != nil {
			return nil, err
		}
	
		// Add dstFields to dst array
		results = append(results, singleResult.Interface())
	}

	return results, nil
}

/*
// Executes a SQL statement (insert, update, delete) and returns its result.
// Parameters in sql start with a colon and will be substituted by the
// corresponding field in the param object. If there are no substitutions,
// pass nil as param.
//
// Example:
// param := DeleteUserByIdQuery{UserId: 42}
// result, err := dapper.Exec(db, "delete from users where id=:UserId", param)
func Exec(db *sql.DB, sql string, param interface{}) (*sql.Result, error) {
	return nil, nil
}
*/

/*
// Same as Exec, but executed in a transaction.
func ExecTx(tx *sql.Tx, sql string, param interface{}) (*sql.Result, error) {
	return nil, nil
}
*/

// Runs the query and returns the value of the first column of the first
// row. This is useful for queries such as counting.
// 
// Parameters in sql start with a colon and will be substituted by the
// corresponding field in the param object. If there are no substitutions,
// pass nil as param.
//
// Notice that sql.ErrNoRows is returned (just as with sql.QueryRow) when
// there is no row found.
// 
// Example:
// param := UserByIdQuery{Id: 42}
// count, err := dapper.Scalar(db, "select count(*) from users where id=:Id", param)
func Scalar(db *sql.DB, sqlQuery string, param interface{}) (interface{}, error) {
	// Get information about param
	if param != nil {
		paramValue := reflect.ValueOf(param)
		paramInfo, err := AddType(paramValue.Type())
		if err != nil {
			return nil, err
		}

		// Substitute parameters in SQL statement
		for paramName, _ := range paramInfo.FieldInfos {
			// Get value of field in param
			field := paramValue.FieldByName(paramName)
			// TODO check for nil and invalid field
			value := field.Interface()
			quoted := Quote(value)
			sqlQuery = strings.Replace(sqlQuery, ":"+paramName, quoted, -1)
		}
	}

	row := db.QueryRow(sqlQuery)

	var dst interface{}
	err := row.Scan(&dst)
	if err != nil {
		return nil, err
	}

	return dst, nil
}

// Count is a helper function that runs Scalar and interprets
// the result as int64. If the result is not an int64, it returns
// ErrWrongType.
func Count(db *sql.DB, sqlQuery string, param interface{}) (int64, error) {
	scalar, err := Scalar(db, sqlQuery, param)
	if err != nil {
		return 0, err
	}
	if count, ok := scalar.(int64); ok {
		return count, nil
	}
	return 0, ErrWrongType
}
