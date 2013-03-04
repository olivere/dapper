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
	ErrNoTableName = errors.New("dapper: no table name specified")
	ErrNoPrimaryKey = errors.New("dapper: no primary key column specified")
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

// ---- Single ---------------------------------------------------------------

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

// ---- All -----------------------------------------------------------------

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

	// We accept both slices of structs or slices of pointers to structs
	elemIsPtr := elemt.Kind() == reflect.Ptr

	gotype := elemt
	if elemIsPtr {
		gotype = elemt.Elem()
	}

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

	rows, err := q.db.Query(sqlQuery)
	if err != nil {
		return err
	}

	i := 0
	var placeholder interface{}
	for rows.Next() {
		// Prepare destination fields for Scan
		singleResult := reflect.New(gotype)

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
		if elemIsPtr {
			slicev = reflect.Append(slicev, singleResult.Elem().Addr())
		} else {
			slicev = reflect.Append(slicev, singleResult.Elem())
		}

		i++
	}

	resultv.Elem().Set(slicev.Slice(0, i))

	return nil
}

// ---- Scalar --------------------------------------------------------------

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

// ---- Count ---------------------------------------------------------------

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

// ---- Insert --------------------------------------------------------------

// Insert adds the entity to the database.
func (s *Session) Insert(entity interface{}) error {
	return s.insert(entity, nil)
}

// InsertTx adds the entity to the database.
func (s *Session) InsertTx(tx *sql.Tx, entity interface{}) error {
	return s.insert(entity, tx)
}

// Insert adds the entity to the database.
func (s *Session) insert(entity interface{}, tx *sql.Tx) error {
	// Get information about the entity
	entityv := reflect.ValueOf(entity)
	if entityv.Kind() != reflect.Ptr {
		return errors.New("entity must be a pointer to a struct")
	}

	indirectValue := reflect.Indirect(entityv)
	gotype := indirectValue.Type()

	ti, err := AddType(gotype)
	if err != nil {
		return err
	}

	// Generate SQL query for insert
	sql, err := s.generateInsertSql(ti, entity)
	if err != nil {
		return err
	}

	// Execute SQL query and return its result
	res, err := s.exec(tx, sql)
	if err != nil {
		return err
	}

	// Set last insert id if the type has an autoincrement column
	if fi, found := ti.HasAutoIncrement(); found {
		newId, err := res.LastInsertId()
		if err != nil {
			return err
		}
		// Set autoincrement column to newly generated Id
		field := entityv.Elem().FieldByName(fi.FieldName)
		field.Set(reflect.ValueOf(newId))
	}

	return nil
}

func (s *Session) exec(tx *sql.Tx, sql string) (sql.Result, error) {
	if tx == nil {
		return s.db.Exec(sql)
	}
	return tx.Exec(sql)
}

func (s *Session) generateInsertSql(ti *typeInfo, entity interface{}) (string, error) {
	if ti.TableName == "" {
		return "", ErrNoTableName
	}

	entityv := reflect.ValueOf(entity)

	cnames := make([]string, 0)
	cvals := make([]string, 0)

	for _, cname := range ti.ColumnNames {
		if fi, found := ti.ColumnInfos[cname]; found {
			if !fi.IsAutoIncrement || fi.IsTransient {
				cnames = append(cnames, cname)

				field := entityv.Elem().FieldByName(fi.FieldName)
				value := field.Interface()
				quoted := Quote(value)
				cvals = append(cvals, quoted)
			}
		}
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		QuoteString(ti.TableName),
		strings.Join(cnames, ", "),
		strings.Join(cvals, ", ")), nil
}

// ---- Update --------------------------------------------------------------

// Update changes an already existing entity in the database.
func (s *Session) Update(entity interface{}) error {
	return s.update(entity, nil)
}

// UpdateTx changes an already existing entity in the database, but runs
// in a transaction.
func (s *Session) UpdateTx(tx *sql.Tx, entity interface{}) error {
	return s.update(entity, tx)
}

// Update changes an already existing entity in the database.
func (s *Session) update(entity interface{}, tx *sql.Tx) error {
	// Get information about the entity
	entityv := reflect.ValueOf(entity)
	entityIsPtr := entityv.Kind() == reflect.Ptr

	gotype := entityv.Type()
	if entityIsPtr {
		gotype = entityv.Type().Elem()
	}

	ti, err := AddType(gotype)
	if err != nil {
		return err
	}

	// Generate SQL query for update
	sql, err := s.generateUpdateSql(ti, entity)
	if err != nil {
		return err
	}

	if tx == nil {
		// Execute SQL query and return its result
		_, err = s.db.Exec(sql)
		if err != nil {
			return err
		}
	} else {
		// Execute SQL query and return its result
		_, err = tx.Exec(sql)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Session) generateUpdateSql(ti *typeInfo, entity interface{}) (string, error) {
	if ti.TableName == "" {
		return "", ErrNoTableName
	}

	entityv := reflect.ValueOf(entity)
	if entityv.Kind() == reflect.Ptr {
		entityv = entityv.Elem()
	}

	pk, found := ti.HasPrimaryKey()
	if !found {
		return "", ErrNoPrimaryKey
	}
	field := entityv.FieldByName(pk.FieldName)
	pkval := field.Interface()

	pairs := make([]string, 0)

	for _, cname := range ti.ColumnNames {
		if fi, found := ti.ColumnInfos[cname]; found {
			if !fi.IsPrimaryKey || fi.IsTransient {
				field = entityv.FieldByName(fi.FieldName)
				value := field.Interface()
				quoted := Quote(value)
				pair := fmt.Sprintf("%s=%s", cname, quoted)
				pairs = append(pairs, pair)
			}
		}
	}

	return fmt.Sprintf("UPDATE %s SET %s WHERE %s=%s",
		QuoteString(ti.TableName),
		strings.Join(pairs, ", "),
		pk.ColumnName,
		Quote(pkval)), nil
}

// ---- Delete --------------------------------------------------------------

// Delete removes the entity from the database.
func (s *Session) Delete(entity interface{}) error {
	return s.delete(entity, nil)
}

// DeleteTx removes the entity from the database, but runs in a transaction.
func (s *Session) DeleteTx(tx *sql.Tx, entity interface{}) error {
	return s.delete(entity, tx)
}

// Delete removes the entity from the database.
func (s *Session) delete(entity interface{}, tx *sql.Tx) error {
	// Get information about the entity
	entityv := reflect.ValueOf(entity)
	entityIsPtr := entityv.Kind() == reflect.Ptr

	gotype := entityv.Type()
	if entityIsPtr {
		gotype = entityv.Type().Elem()
	}

	ti, err := AddType(gotype)
	if err != nil {
		return err
	}

	// Generate SQL query for delete
	sql, err := s.generateDeleteSql(ti, entity)
	if err != nil {
		return err
	}

	if tx == nil {
		// Execute SQL query and return its result
		_, err = s.db.Exec(sql)
		if err != nil {
			return err
		}
	} else {
		// Execute SQL query n transaction and return its result
		_, err = tx.Exec(sql)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Session) generateDeleteSql(ti *typeInfo, entity interface{}) (string, error) {
	if ti.TableName == "" {
		return "", ErrNoTableName
	}

	entityv := reflect.ValueOf(entity)
	if entityv.Kind() == reflect.Ptr {
		entityv = entityv.Elem()
	}

	pk, found := ti.HasPrimaryKey()
	if !found {
		return "", ErrNoPrimaryKey
	}
	field := entityv.FieldByName(pk.FieldName)
	pkval := field.Interface()

	return fmt.Sprintf("DELETE FROM %s WHERE %s=%s",
		QuoteString(ti.TableName),
		pk.ColumnName,
		Quote(pkval)), nil
}

