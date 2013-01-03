package dapper

import (
	"bytes"
	"fmt"
	"reflect"
)

// Represents a SQL query on a SQL database.

type query struct {
	t            *tableClause
	columns      []string
	joins        []*joinClause
	where        *whereClause
	limit        *limitClause
	orders       []*orderClause
}

func Q(table string) *query {
	q := &query{}
	t := NewTableClause(q, table)
	q.t = t
	q.columns = make([]string, 1)
	q.columns[0] = "*"
	q.joins = make([]*joinClause, 0)
	q.orders = make([]*orderClause, 0)
	return q
}

func (q *query) Alias(alias string) *query {
	q.t = q.t.Alias(alias)
	return q
}

func (q *query) Project(columns ...string) *query {
	q.columns = make([]string, 0)
	q.columns = append(q.columns, columns...)
	return q
}

func (q *query) Where() *whereClause {
	q.where = NewWhereClause(q)
	return q.where
}

func (q *query) Join(table string) *joinClause {
	t := NewTableClause(q, table)
	j := NewJoinClause(q, t, "")
	q.joins = append(q.joins, j)
	return j
}

func (q *query) InnerJoin(table string) *joinClause {
	t := NewTableClause(q, table)
	j := NewJoinClause(q, t, "INNER")
	q.joins = append(q.joins, j)
	return j
}

func (q *query) OuterJoin(table string) *joinClause {
	t := NewTableClause(q, table)
	j := NewJoinClause(q, t, "OUTER")
	q.joins = append(q.joins, j)
	return j
}

func (q *query) LeftInnerJoin(table string) *joinClause {
	t := NewTableClause(q, table)
	j := NewJoinClause(q, t, "LEFT INNER")
	q.joins = append(q.joins, j)
	return j
}

func (q *query) LeftOuterJoin(table string) *joinClause {
	t := NewTableClause(q, table)
	j := NewJoinClause(q, t, "LEFT OUTER")
	q.joins = append(q.joins, j)
	return j
}

func (q *query) Order() *orderClause {
	c := NewOrderClause(q)
	q.orders = append(q.orders, c)
	return c
}

func (q *query) Take(take int) *query {
	if q.limit == nil {
		q.limit = &limitClause{}
	}
	q.limit.Take(take)
	return q
}

func (q *query) Skip(skip int) *query {
	if q.limit == nil {
		q.limit = &limitClause{}
	}
	q.limit.Skip(skip)
	return q
}

func (q *query) Sql() string {
	var b bytes.Buffer
	b.WriteString("SELECT ")
	for i, column := range q.columns {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(QuoteString(column))
	}
	b.WriteString(" FROM ")
	b.WriteString(q.t.SubSql())
	if len(q.joins) > 0 {
		b.WriteString(" ")
		for i, join := range q.joins {
			if i > 0 {
				b.WriteString(" ")
			}
			b.WriteString(join.SubSql())
		}
	}
	if q.where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(q.where.SubSql())
	}
	if len(q.orders) > 0 {
		b.WriteString(" ORDER BY ")
		for i, order := range q.orders {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(order.SubSql())
		}
	}
	if q.limit != nil {
		b.WriteString(" ")
		b.WriteString(q.limit.SubSql())
	}
	return b.String()
}

// Tables

type tableClause struct {
	q     *query
	name  string
	alias string
}

func NewTableClause(q *query, name string) *tableClause {
	return &tableClause{q, name, ""}
}

func (t *tableClause) Name(name string) *tableClause {
	t.name = name
	return t
}

func (t *tableClause) Alias(alias string) *tableClause {
	t.alias = alias
	return t
}

func (t *tableClause) Project(columns ...string) *query {
	return t.q.Project(columns...)
}

func (t *tableClause) Take(take int) *query {
	return t.q.Take(take)
}

func (t *tableClause) Skip(take int) *query {
	return t.q.Skip(take)
}

func (t *tableClause) Sql() string {
	return t.q.Sql()
}

func (t *tableClause) SubSql() string {
	var b bytes.Buffer
	b.WriteString(QuoteString(t.name))
	if t.alias != "" {
		b.WriteString(" ")
		b.WriteString(QuoteString(t.alias))
	}
	return b.String()
}

// Joins

type joinClause struct {
	q  *query
	t  *tableClause
	kind string
	left,right string
}

func NewJoinClause(q *query, t *tableClause, kind string) *joinClause {
	return &joinClause{q, t, kind, "", ""}
}

func (j *joinClause) Kind(kind string) *joinClause {
	j.kind = kind
	return j
}

func (j *joinClause) Alias(alias string) *joinClause {
	j.t = j.t.Alias(alias)
	return j
}

func (j *joinClause) On(left, right string) *joinClause {
	j.left = left
	j.right = right
	return j
}

func (j *joinClause) Join(table string) *joinClause {
	return j.q.Join(table)
}

func (j *joinClause) Project(columns ...string) *query {
	return j.q.Project(columns...)
}

func (j *joinClause) Take(take int) *query {
	return j.q.Take(take)
}

func (j *joinClause) Skip(take int) *query {
	return j.q.Skip(take)
}

func (j *joinClause) Sql() string {
	return j.q.Sql()
}

func (j *joinClause) SubSql() string {
	var b bytes.Buffer
	if j.kind != "" {
		b.WriteString(j.kind)
		b.WriteString(" ")
	}
	b.WriteString("JOIN ")
	b.WriteString(j.t.SubSql())
	b.WriteString(" ON ")
	b.WriteString(j.left)
	b.WriteString("=")
	b.WriteString(j.right)
	return b.String()
}

// Where clauses

type whereClause struct {
	q     *query
	nodes []whereNode
}

func NewWhereClause(query *query) *whereClause {
	wc := &whereClause{
		q: query,
		nodes: make([]whereNode, 0),
	}
	return wc
}

func (wc *whereClause) Eq(column string, value interface{}) *whereClause {
	we := whereEqual{wc.q, column, value}
	wc.nodes = append(wc.nodes, we)
	return wc
}

func (wc *whereClause) Ne(column string, value interface{}) *whereClause {
	wne := whereNotEqual{wc.q, column, value}
	wc.nodes = append(wc.nodes, wne)
	return wc
}

func (wc *whereClause) Like(column string, value interface{}) *whereClause {
	c := whereLike{wc.q, column, value}
	wc.nodes = append(wc.nodes, c)
	return wc
}

func (wc *whereClause) NotLike(column string, value interface{}) *whereClause {
	c := whereNotLike{wc.q, column, value}
	wc.nodes = append(wc.nodes, c)
	return wc
}

func (wc *whereClause) In(column string, values ...interface{}) *whereClause {
	c := whereIn{wc.q, column, values}
	wc.nodes = append(wc.nodes, c)
	return wc
}

func (wc *whereClause) NotIn(column string, values ...interface{}) *whereClause {
	c := whereNotIn{wc.q, column, values}
	wc.nodes = append(wc.nodes, c)
	return wc
}

func (wc *whereClause) Project(columns ...string) *query {
	return wc.q.Project(columns...)
}

func (wc *whereClause) Take(take int) *query {
	return wc.q.Take(take)
}

func (wc *whereClause) Skip(take int) *query {
	return wc.q.Skip(take)
}

func (wc *whereClause) Order() *orderClause {
	return wc.q.Order()
}

func (wc *whereClause) Sql() string {
	return wc.q.Sql()
}

func (wc *whereClause) SubSql() string {
	var b bytes.Buffer
	for i, node := range wc.nodes {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString(node.SubSql())
	}
	return b.String()
}

// WhereNodes specify a node in a where clause
type whereNode interface {
	Sql() string
	SubSql() string
}

// A where clause of type "column = value"

type whereEqual struct {
	q       *query
	column  string
	value   interface{}
}

func (we whereEqual) Sql() string {
	return we.q.Sql()
}

func (we whereEqual) SubSql() string {
	if we.value != nil {
		return fmt.Sprintf("%s%s%s", we.column, "=", Quote(we.value))
	}
	return fmt.Sprintf("%s IS NULL", we.column)
}

// A where clause of type "column != value"

type whereNotEqual struct {
	q       *query
	column  string
	value   interface{}
}

func (wne whereNotEqual) Sql() string {
	return wne.q.Sql()
}

func (wne whereNotEqual) SubSql() string {
	if wne.value != nil {
		return fmt.Sprintf("%s%s%s", wne.column, "<>", Quote(wne.value))
	}
	return fmt.Sprintf("%s IS NOT NULL", wne.column)
}

// A where clause of type "column LIKE value"

type whereLike struct {
	q       *query
	column  string
	value   interface{}
}

func (w whereLike) Sql() string {
	return w.q.Sql()
}

func (w whereLike) SubSql() string {
	return fmt.Sprintf("%s LIKE %s", w.column, Quote(w.value))
}

// A where clause of type "column NOT LIKE value"

type whereNotLike struct {
	q       *query
	column  string
	value   interface{}
}

func (w whereNotLike) Sql() string {
	return w.q.Sql()
}

func (w whereNotLike) SubSql() string {
	return fmt.Sprintf("%s NOT LIKE %s", w.column, Quote(w.value))
}

// A where clause of type "column IN (...)"

type whereIn struct {
	q       *query
	column  string
	values  []interface{}
}

func (w whereIn) Sql() string {
	return w.q.Sql()
}

func (w whereIn) SubSql() string {
	var b bytes.Buffer
	for i, value := range w.values {
		// The element itself could be an array or a slice
		inv := reflect.ValueOf(value)
		if inv.Kind() == reflect.Slice || inv.Kind() == reflect.Array {
			invlen := inv.Len()
			for j := 0; j < invlen; j++ {
				if j > 0 {
					b.WriteString(",")
				}
				b.WriteString(Quote(inv.Index(j).Interface()))
			}
		} else {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(Quote(value))
		}
	}
	return fmt.Sprintf("%s IN (%s)", w.column, b.String())
}

// A where clause of type "column NOT IN (...)"

type whereNotIn struct {
	q       *query
	column  string
	values  []interface{}
}

func (w whereNotIn) Sql() string {
	return w.q.Sql()
}

func (w whereNotIn) SubSql() string {
	var b bytes.Buffer
	for i, value := range w.values {
		// The element itself could be an array or a slice
		inv := reflect.ValueOf(value)
		if inv.Kind() == reflect.Slice || inv.Kind() == reflect.Array {
			invlen := inv.Len()
			for j := 0; j < invlen; j++ {
				if j > 0 {
					b.WriteString(",")
				}
				b.WriteString(Quote(inv.Index(j).Interface()))
			}
		} else {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(Quote(value))
		}
	}
	return fmt.Sprintf("%s NOT IN (%s)", w.column, b.String())
}

// Order clause

type orderClause struct {
	q    *query
	col  string
	dir  string
}

func NewOrderClause(query *query) *orderClause {
	c := &orderClause{
		q:   query,
		col: "",
		dir: "",
	}
	return c
}

func (c *orderClause) Asc(column string) *orderClause {
	c.col = column
	c.dir = "ASC"
	return c
}

func (c *orderClause) Desc(column string) *orderClause {
	c.col = column
	c.dir = "DESC"
	return c
}

func (c *orderClause) Order() *orderClause {
	return c.q.Order()
}

func (c *orderClause) Take(take int) *query {
	return c.q.Take(take)
}

func (c *orderClause) Skip(take int) *query {
	return c.q.Skip(take)
}

func (c *orderClause) Sql() string {
	return c.q.Sql()
}

func (c *orderClause) SubSql() string {
	return fmt.Sprintf("%s %s", c.col, c.dir)
}

// Limit clause

type limitClause struct {
	query   *query
	skip    int
	take    int
}

func NewLimitClause(query *query) *limitClause {
	lc := &limitClause{
		query:  query,
		skip: -1,
		take: -1,
	}
	return lc
}

func (lc *limitClause) Skip(skip int) *limitClause {
	lc.skip = skip
	return lc
}

func (lc *limitClause) Take(take int) *limitClause {
	lc.take = take
	return lc
}

func (lc *limitClause) Sql() string {
	return lc.query.Sql()
}

func (lc *limitClause) SubSql() string {
	if lc.take < 0 && lc.skip < 0 {
		return ""
	}
	var b bytes.Buffer
	b.WriteString("LIMIT ")
	if lc.skip > 0 {
		b.WriteString(fmt.Sprintf("%d", lc.skip))
		if lc.take >= 0 {
			b.WriteString(",")
			b.WriteString(fmt.Sprintf("%d", lc.take))
		}
	} else {
		b.WriteString(fmt.Sprintf("%d", lc.take))
	}
	return b.String()
}
