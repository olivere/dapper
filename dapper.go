package dapper

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

var (
	ErrWrongType = errors.New("dapper: wrong type")
)

// Session represents an interface to a database.
type Session struct {
	db *sql.DB
}

// Finder is a type for querying the database.
type finder struct {
	session *Session
	db *sql.DB
	sqlQuery string
	param interface{}
}

// New creates a Session from a database connection.
func New(db *sql.DB) *Session {
	return &Session{db}
}

// Find opens up the query interface of a Session.
// Parameters in sql start with a colon and will be substituted by the
// corresponding field in the param object. If there are no substitutions,
// pass nil as param.
func (s *Session) Find(sql string, param interface{}) *finder {
	return &finder{s, s.db, sql, param}
}

// Single returns the first result of the SQL query in result.
//
// If no rows are found, sql.ErrNoRows is returned (see sql.QueryRow).
// 
// Example:
// param := UserByIdQuery{Id: 42}
// var result User{}
// err := session.Find("select * from users where id=:Id", param).Single(&result)
func (q *finder) Single(result interface{}) error {
	// Get information about result
	resultValue := reflect.ValueOf(result)
	if resultValue.Kind() != reflect.Ptr {
		return errors.New("result must be a pointer to a struct")
	}

	indirectValue := reflect.Indirect(resultValue)
	gotype := indirectValue.Type()

	resultInfo, err := AddType(gotype)
	if err != nil {
		return err
	}

	// Get information about param
	sqlQuery := q.sqlQuery
	if q.param != nil {
		paramValue := reflect.ValueOf(q.param)
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
	rows, err := q.db.Query(sqlQuery)
	if err != nil {
		return err
	}

	// Scan fills all fields in dst here
	var placeholder interface{}
	if rows.Next() {
		resultFields := make([]interface{}, 0)
		dbColumnNames, err := rows.Columns()
		if err != nil {
			return err
		}
		for _, dbColName := range dbColumnNames {
			fi, found := resultInfo.ColumnInfos[dbColName]
			if found {
				field := resultValue.Elem().FieldByName(fi.FieldName)
				resultFields = append(resultFields, field.Addr().Interface())
			} else {
				// Ignore missing columns
				resultFields = append(resultFields, &placeholder)
				/*
				return errors.New(
					fmt.Sprintf("type %s: found no corresponding mapping "+
						"for column %s in result", gotype, dbColName))
				*/
			}
		}

		// Scan results
		err = rows.Scan(resultFields...)
		if err != nil {
			return err
		}
	} else {
		// If there's no row, we should return sql.ErrNoRows
		return sql.ErrNoRows
	}

	return nil
}

// All returns a slice of results of the SQL query in result.
// The result parameter must be a pointer to a slice of query results.
// If no rows are found, sql.ErrNoRows is returned.
//
// Example:
// param := UserByCompanyQuery{CompanyId: 42}
// var results []UserByCompanyQuery
// err := session.Find("select * from users "+
//     "where company_id=:CompanyId "+
//     "order by email limit 10", param).All(&results)
func (q *finder) All(result interface{}) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		return errors.New("result must be a pointer to a slice")
	}

	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()

	if elemt.Kind() == reflect.Ptr {
		return errors.New("element type of result slice must not be a pointer")
	}

	resultInfo, err := AddType(elemt)
	if err != nil {
		return err
	}

	// Get information about param
	sqlQuery := q.sqlQuery
	if q.param != nil {
		paramValue := reflect.ValueOf(q.param)
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

	rows, err := q.db.Query(sqlQuery)
	if err != nil {
		return err
	}

	i := 0
	var placeholder interface{}
	for rows.Next() {
		// Prepare destination fields for Scan
		singleResult := reflect.New(elemt)

		resultFields := make([]interface{}, 0)
		dbColumnNames, err := rows.Columns()
		if err != nil {
			return err
		}
		for _, dbColName := range dbColumnNames {
			fi, found := resultInfo.ColumnInfos[dbColName]
			if found {
				field := singleResult.Elem().FieldByName(fi.FieldName)
				resultFields = append(resultFields, field.Addr().Interface())
			} else {
				// Ignore missing columns
				resultFields = append(resultFields, &placeholder)
				/*
				return nil, errors.New(
					fmt.Sprintf("type %s: found no corresponding mapping "+
						"for column %s in result", gotype, dbColName))
				//*/
			}
		}

		// Scan fills all fields in singleResult here
		err = rows.Scan(resultFields...)
		if err != nil {
			return err
		}

		// Add resultFields to slice
		slicev = reflect.Append(slicev, singleResult.Elem())

		i++
	}

	resultv.Elem().Set(slicev.Slice(0, i))

	return nil
}

// Scalar runs the finder query and returns the value of the first column 
// of the first row. This is useful for queries such as counting.
// 
// The result parameter must be a pointer to a matching value.
// If no rows are found, sql.ErrNoRows is returned.
// 
// Example:
// param := UserByIdQuery{Id: 42}
// var count int64
// err := session.Find("select count(*) from users where id=:Id", param).Scalar(&count)
func (q *finder) Scalar(result interface{}) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr {
		return errors.New("result must be a pointer")
	}

	sqlQuery := q.sqlQuery

	// Get information about param
	if q.param != nil {
		paramValue := reflect.ValueOf(q.param)
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

	row := q.db.QueryRow(sqlQuery)

	elemt := resultv.Type().Elem()
	value := reflect.New(elemt)
	err := row.Scan(value.Interface())
	if err != nil {
		return err
	}

	resultv.Elem().Set(value.Elem())

	return nil
}

// Count returns the count of the query as an int64.
// If the result is not an int64, it returns ErrWrongType.
// 
// Example:
// count, err := session.Count("select count(*) from users", nil)
func (s *Session) Count(sqlQuery string, param interface{}) (int64, error) {
	var result interface{}
	err := s.Find(sqlQuery, param).Scalar(&result)
	if err != nil {
		return 0, err
	}
	if count, ok := result.(int64); ok {
		return count, nil
	}
	return 0, ErrWrongType
}
