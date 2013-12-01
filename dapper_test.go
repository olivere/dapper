package dapper

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/ziutek/mymysql/godrv"
)

const (
	testDBName = "dapper_test"
	testDBUser = "dapper"
	testDBPass = "dapper"
)

var (
	drivers = []string{"mymysql", "mysql", "sqlite3"}
)

// ---- Test tables ----------------------------------------------------------

type cruddy struct {
	Id          int64      `dapper:"id,primarykey,autoincrement,table=cruddy"`
	Int         int        `dapper:"c_int"`
	Int32       int32      `dapper:"c_int32"`
	Int64       int64      `dapper:"c_int64"`
	Uint        uint       `dapper:"c_uint"`
	Uint32      uint32     `dapper:"c_uint32"`
	Uint64      uint64     `dapper:"c_uint64"`
	Float32     float32    `dapper:"c_float32"`
	Float64     float64    `dapper:"c_float64"`
	Decimal     float64    `dapper:"c_decimal"`
	DateTime    time.Time  `dapper:"c_datetime"`
	DateTimePtr *time.Time `dapper:"c_datetime_ptr"`
	Timestamp   *time.Time `dapper:"c_timestamp"`
	Bool        bool       `dapper:"c_bool"`
	Char        string     `dapper:"c_char"`
	Varchar     string     `dapper:"c_varchar"`
	Text        string     `dapper:"c_text"`
}

type tweet struct {
	Id       int64  `dapper:"id,primarykey,autoincrement,table=tweets"`
	UserId   int64  `dapper:"user_id"`
	Message  string `dapper:"message"`
	Retweets int64  `dapper:"retweets"`
	//Created    time.Time `dapper:"created"`
	CreatedStr string `dapper:"created"`
}

type tweetById struct {
	Id int64
}

type tweetByUserId struct {
	UserId int64
}

type tweetByUserAndMinRetweets struct {
	UserId      int64
	NumRetweets int64
}

type sampleQuery struct {
	Id     int64  `dapper:"id,primarykey,autoincrement"`
	Ignore string `dapper:"-"`
	UserId int64
}

func (t *tweet) String() string {
	return fmt.Sprintf("tweet[Id=%v,UserId=%v,Message=%v,Retweets=%v,Created=%v]",
		t.Id, t.UserId, t.Message, t.Retweets, t.Created())
}

func (t *tweet) Created() *time.Time {
	tm, err := time.Parse(time.RFC3339, t.CreatedStr)
	if err != nil {
		return nil
	}
	return &tm
}

type validater interface {
	Validate() bool
}

type user struct {
	validater
	Id        int64    `dapper:"id,primarykey,autoincrement,table=users"`
	Name      string   `dapper:"name"`
	Karma     *float64 `dapper:"karma"`
	Suspended bool     `dapper:"suspended"`
}

type userWithoutTableNameTag struct {
	Id        int64    `dapper:"id,primarykey,autoincrement"`
	Name      string   `dapper:"name"`
	Karma     *float64 `dapper:"karma"`
	Suspended bool     `dapper:"suspended"`
}

type userWithoutPrimaryKeyTag struct {
	Id        int64    `dapper:"id,autoincrement,table=users"`
	Name      string   `dapper:"name"`
	Karma     *float64 `dapper:"karma"`
	Suspended bool     `dapper:"suspended"`
}

type userWithMissingColumns struct {
	Id   int64  `dapper:"id,primarykey,autoincrement,table=users"`
	Name string `dapper:"name"`
}

func (u *user) String() string {
	return fmt.Sprintf("user[Id=%v,Name=%v,Karma=%v,Suspended=%v]",
		u.Id, u.Name, u.Karma, u.Suspended)
}

func (u *user) Validate() bool {
	return u.Name != ""
}

type Order struct {
	Id    int64        `dapper:"id,primarykey,autoincrement,table=orders"`
	RefId string       `dapper:"ref_id"`
	User  *user        `dapper:"-"`
	Items []*OrderItem `dapper:"oneToMany=OrderId"`
}

func (o Order) String() string {
	return fmt.Sprintf("<Order{Id:%d,RefId:%s,len(Items):%d}>", o.Id, o.RefId, len(o.Items))
}

type OrderItem struct {
	Id      int64   `dapper:"id,primarykey,autoincrement,table=order_items"`
	OrderId int64   `dapper:"order_id"`
	Order   *Order  `dapper:"oneToOne=OrderId"`
	Name    string  `dapper:"name"`
	Price   float64 `dapper:"price"`
	Qty     float64 `dapper:"qty"`
}

func (item OrderItem) String() string {
	return fmt.Sprintf("<OrderItem{Id:%d,OrderId:%d,Name:%s,Order:%v}>",
		item.Id, item.OrderId, item.Name, item.Order)
}

// -- Setup -----------------------------------------------------------------

func setup(driver string, t *testing.T) (db *sql.DB) {
	var err error
	switch driver {
	case "mymysql":
		connectionString := fmt.Sprintf("%s/%s/%s", testDBName, testDBUser, testDBPass)
		db, err = sql.Open("mymysql", connectionString)
		if err != nil {
			t.Fatalf("error connection to database: %v", err)
		}
	case "mysql":
		connectionString := fmt.Sprintf("%s:%s@/%s?charset=utf8", testDBUser, testDBPass, testDBName)
		db, err = sql.Open("mysql", connectionString)
		if err != nil {
			t.Fatalf("error connection to database: %v", err)
		}
	case "sqlite3":
		os.Remove("./" + testDBName + ".db")
		connectionString := fmt.Sprintf("./%s.db", testDBName)
		db, err = sql.Open("sqlite3", connectionString)
		if err != nil {
			t.Fatalf("error connection to database: %v", err)
		}
	}
	return seed(driver, t, db)
}

func seed(driver string, t *testing.T, db *sql.DB) *sql.DB {
	// Drop tables
	_, err := db.Exec("DROP TABLE IF EXISTS tweets")
	if err != nil {
		t.Fatalf("error dropping tweets table: %v", err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Fatalf("error dropping users table: %v", err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS cruddy")
	if err != nil {
		t.Fatalf("error dropping cruddy table: %v", err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS order_items")
	if err != nil {
		t.Fatalf("error dropping order_items table: %v", err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS orders")
	if err != nil {
		t.Fatalf("error dropping orders table: %v", err)
	}

	// Create tables
	autoinc := "AUTO_INCREMENT"
	if driver == "sqlite3" {
		autoinc = "AUTOINCREMENT"
	}
	timestampCol := "timestamp NOT NULL " +
		"DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
	if driver == "sqlite3" {
		timestampCol = "datetime NOT NULL " +
			"DEFAULT CURRENT_TIMESTAMP"
	}
	_, err = db.Exec(`
CREATE TABLE cruddy (
    id integer not null primary key ` + autoinc + `,
		c_int integer,
		c_int32 integer,
		c_int64 integer,
		c_uint  integer,
		c_uint32 integer,
		c_uint64 integer,
		c_float32 float,
		c_float64 float,
		c_decimal decimal(19,5),
		c_date date,
		c_date_ptr date,
		c_datetime datetime,
		c_datetime_ptr datetime,
		c_timestamp ` + timestampCol + `,
		c_bool bool,
		c_char char(3),
		c_varchar varchar(20),
		c_text text
)`)
	if err != nil {
		t.Fatalf("error creating cruddy table: %v", err)
	}

	_, err = db.Exec(`
CREATE TABLE users (
        id integer not null primary key ` + autoinc + `,
        name varchar(100) not null,
        karma decimal(19,5),
        suspended tinyint(1) default '0'
)`)
	if err != nil {
		t.Fatalf("error creating users table: %v", err)
	}

	_, err = db.Exec(`
CREATE TABLE tweets (
        id integer not null primary key  ` + autoinc + `,
        user_id integer not null,
        message text,
        retweets integer,
        created timestamp not null default current_timestamp,
        foreign key (user_id) references users (id) on delete cascade
)`)
	if err != nil {
		t.Fatalf("error creating tweets table: %v", err)
	}

	_, err = db.Exec(`
CREATE TABLE orders (
        id integer not null primary key ` + autoinc + `,
        ref_id varchar(100) not null
)`)
	if err != nil {
		t.Fatalf("error creating orders table: %v", err)
	}

	_, err = db.Exec(`
CREATE TABLE order_items (
        id integer not null primary key ` + autoinc + `,
        order_id integer not null,
        name varchar(100) not null,
        price decimal(15,3) not null,
        qty decimal(10,3) not null,
        foreign key (order_id) references orders (id) on delete cascade
)`)
	if err != nil {
		t.Fatalf("error creating order_items table: %v", err)
	}

	// Insert seed data
	_, err = db.Exec("INSERT INTO users (id,name,karma,suspended) VALUES (1, 'Oliver', 42.13, 0)")
	if err != nil {
		t.Fatalf("error inserting user: %v", err)
	}
	_, err = db.Exec("INSERT INTO users (id,name,karma,suspended) VALUES (2, 'Sandra', 57.19, 1)")
	if err != nil {
		t.Fatalf("error inserting user: %v", err)
	}

	_, err = db.Exec("INSERT INTO tweets (id,user_id,message,retweets) VALUES (1, 1, 'Google Go rocks', 179)")
	if err != nil {
		t.Fatalf("error inserting tweet: %v", err)
	}
	_, err = db.Exec("INSERT INTO tweets (id,user_id,message,retweets) VALUES (2, 1, '... so does Google Maps', 19)")
	if err != nil {
		t.Fatalf("error inserting tweet: %v", err)
	}
	_, err = db.Exec("INSERT INTO tweets (id,user_id,message,retweets) VALUES (3, 2, 'Holidays! Yay!', 1)")
	if err != nil {
		t.Fatalf("error inserting tweet: %v", err)
	}

	_, err = db.Exec("INSERT INTO orders (id,ref_id) VALUES (1, 'APPLE1')")
	if err != nil {
		t.Fatalf("error inserting order: %v", err)
	}
	_, err = db.Exec("INSERT INTO orders (id,ref_id) VALUES (2, 'OFFICE1')")
	if err != nil {
		t.Fatalf("error inserting order: %v", err)
	}
	_, err = db.Exec("INSERT INTO orders (id,ref_id) VALUES (3, 'EMPTY1')")
	if err != nil {
		t.Fatalf("error inserting order: %v", err)
	}

	_, err = db.Exec("INSERT INTO order_items (id,order_id,name,price,qty) VALUES (1, 1, 'MacBook Air 11\"', 1199.90, 1)")
	if err != nil {
		t.Fatalf("error inserting order item: %v", err)
	}
	_, err = db.Exec("INSERT INTO order_items (id,order_id,name,price,qty) VALUES (2, 1, 'iPad 4th gen.', 499.90, 2)")
	if err != nil {
		t.Fatalf("error inserting order item: %v", err)
	}
	_, err = db.Exec("INSERT INTO order_items (id,order_id,name,price,qty) VALUES (3, 2, 'Lenovo T430s', 1499.90, 1)")
	if err != nil {
		t.Fatalf("error inserting order item: %v", err)
	}
	_, err = db.Exec("INSERT INTO order_items (id,order_id,name,price,qty) VALUES (4, 2, 'BlackBox', 199.90, 20)")
	if err != nil {
		t.Fatalf("error inserting order item: %v", err)
	}

	return db
}

// ---- Types ---------------------------------------------------------------

func TestTypeCache(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		/*
			if len(typeCache) != 0 {
				t.Errorf("expected type cache to be empty, got %d entries", len(typeCache))
			}
		*/

		// Test typeInfo
		ti, err := AddType(reflect.TypeOf(sampleQuery{}))
		if err != nil {
			t.Errorf("error adding type sampleQuery: %v", err)
		}
		if ti == nil {
			t.Errorf("expected to return typeInfo, got nil")
		}
		if len(ti.FieldNames) != 3 {
			t.Errorf("expected typeInfo to have %d fields, got %d", 3, len(ti.FieldNames))
		}

		// Test field Id
		fi, found := ti.FieldInfos["Id"]
		if !found {
			t.Errorf("expected typeInfo to have an Id field")
		}
		if fi.FieldName != "Id" {
			t.Errorf("expected field Id to have name: Id")
		}
		if fi.ColumnName != "id" {
			t.Errorf("expected field Id to have column name: id (lower-case)")
		}
		if !fi.IsPrimaryKey {
			t.Errorf("expected field Id to be primary key")
		}
		if !fi.IsAutoIncrement {
			t.Errorf("expected field Id to be auto-increment")
		}
		if fi.IsTransient {
			t.Errorf("expected field Id to not be transient")
		}

		// Test field UserId
		fi, found = ti.FieldInfos["UserId"]
		if !found {
			t.Errorf("expected typeInfo to have a UserId field")
		}
		if fi.FieldName != "UserId" {
			t.Errorf("expected field UserId to have name: UserId")
		}
		if fi.ColumnName != "UserId" {
			t.Errorf("expected field UserId to have column name: User")
		}
		if fi.IsPrimaryKey {
			t.Errorf("expected field UserId to not be primary key")
		}
		if fi.IsAutoIncrement {
			t.Errorf("expected field UserId to not be auto-increment")
		}
		if fi.IsTransient {
			t.Errorf("expected field UserId to not be transient")
		}

		// Test field Ignore
		fi, found = ti.FieldInfos["Ignore"]
		if !found {
			t.Errorf("expected typeInfo to have an Ignore field")
		}
		if fi.FieldName != "Ignore" {
			t.Errorf("expected field Ignore to have name: Ignore")
		}
		if fi.ColumnName != "" {
			t.Errorf("expected field Ignore to have an empty column name")
		}
		if fi.IsPrimaryKey {
			t.Errorf("expected field Ignore to not be primary key")
		}
		if fi.IsAutoIncrement {
			t.Errorf("expected field Ignore to not be auto-increment")
		}
		if !fi.IsTransient {
			t.Errorf("expected field Ignore to be transient")
		}
	}
}

func TestTypeCacheOneToMany(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		ti, err := AddType(reflect.TypeOf(Order{}))
		if err != nil {
			t.Errorf("error adding type Order: %v", err)
		}
		if ti == nil {
			t.Errorf("expected to return typeInfo, got nil")
		}
		if len(ti.FieldNames) != 3 {
			t.Errorf("expected typeInfo to have %d fields, got %d", 3, len(ti.FieldNames))
		}
		if len(ti.AssocFieldNames) != 1 {
			t.Fatalf("expected len(AssocFieldNames) = %d, got %d", 1, len(ti.AssocFieldNames))
		}
		if ti.AssocFieldNames[0] != "Items" {
			t.Fatalf("expected AssocFieldNames[0] = %s, got %s", "Items", ti.AssocFieldNames[0])
		}

		assoc, found := ti.OneToManyInfos["Items"]
		if !found {
			t.Fatalf("expected to find association by name")
		}
		if assoc.FieldName != "Items" {
			t.Errorf("expected association field name of %s, got %s", "Items", assoc.FieldName)
		}
		sliceSample := make([]*OrderItem, 0)
		var elemSample *OrderItem
		if assoc.SliceType != reflect.TypeOf(sliceSample) {
			t.Errorf("expected association slice type of %s, got %s", reflect.TypeOf(sliceSample).String(), assoc.SliceType.String())
		}
		if assoc.ElemType != reflect.TypeOf(elemSample) {
			t.Fatalf("expected association element type of %s, got %s", reflect.TypeOf(elemSample).String(), assoc.ElemType.String())
		}
		tableName, err := assoc.GetTableName()
		if err != nil {
			t.Fatalf("expected to find table name for association, got %v", err)
		}
		if tableName != "order_items" {
			t.Errorf("expected foreign table name to be %s, got %s", "order_items", tableName)
		}
		columnName, err := assoc.GetColumnName()
		if err != nil {
			t.Fatalf("expected to find column name for association, got %v", err)
		}
		if columnName != "order_id" {
			t.Errorf("expected foreign column name to be %s, got %s", "order_id", columnName)
		}
		if assoc.ForeignKeyField != "OrderId" {
			t.Errorf("expected foreign key field to be %s, got %s", "OrderId", assoc.ForeignKeyField)
		}
	}
}

func TestTypeCacheOneToOne(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		// Order
		ti, err := AddType(reflect.TypeOf(Order{}))
		if err != nil {
			t.Errorf("error adding type Order: %v", err)
		}
		if ti == nil {
			t.Errorf("expected to return typeInfo, got nil")
		}
		if len(ti.FieldNames) != 3 {
			t.Errorf("expected typeInfo to have %d fields, got %d", 3, len(ti.FieldNames))
		}

		// OrderItem
		ti, err = AddType(reflect.TypeOf(OrderItem{}))
		if err != nil {
			t.Errorf("error adding type OrderItem: %v", err)
		}
		if ti == nil {
			t.Errorf("expected to return typeInfo, got nil")
		}
		if len(ti.FieldNames) != 5 {
			t.Errorf("expected typeInfo to have %d fields, got %d", 5, len(ti.FieldNames))
		}

		if len(ti.AssocFieldNames) != 1 {
			t.Fatalf("expected len(AssocFieldNames) = %d, got %d", 1, len(ti.AssocFieldNames))
		}
		if ti.AssocFieldNames[0] != "Order" {
			t.Fatalf("expected AssocFieldNames[0] = %s, got %s", "Order", ti.AssocFieldNames[0])
		}
		assoc, found := ti.OneToOneInfos["Order"]
		if !found {
			t.Fatalf("expected to find association by name")
		}
		if assoc.FieldName != "Order" {
			t.Errorf("expected association field name of %s, got %s", "Order", assoc.FieldName)
		}
		var sample *Order
		if assoc.TargetType != reflect.TypeOf(sample) {
			t.Errorf("expected association type of %s, got %s", reflect.TypeOf(sample).String(), assoc.TargetType.String())
		}
		tableName, err := assoc.GetTableName()
		if err != nil {
			t.Fatalf("expected to find table name for association, got %v", err)
		}
		if tableName != "orders" {
			t.Errorf("expected foreign table name to be %s, got %s", "orders", tableName)
		}
		columnName, err := assoc.GetColumnName()
		if err != nil {
			t.Fatalf("expected to find column name for association, got %v", err)
		}
		if columnName != "id" {
			t.Errorf("expected foreign column name to be %s, got %s", "id", columnName)
		}
		if assoc.ForeignKeyField != "OrderId" {
			t.Errorf("expected foreign column name to be %s, got %s", "OrderId", assoc.ForeignKeyField)
		}
	}
}

// ---- CRUD with all data types ---------------------------------------------

func TestCRUDOnMymysqlDriver(t *testing.T) {
	db := setup("mymysql", t)
	defer db.Close()

	session := New(db)
	now := time.Now()
	in := cruddy{
		Int:         1,
		Int32:       int32(2),
		Int64:       int64(3),
		Uint:        uint(4),
		Uint32:      uint32(5),
		Uint64:      uint64(6),
		Float32:     float32(7.1),
		Float64:     float64(8.2),
		Decimal:     float64(9.33),
		DateTime:    now,
		DateTimePtr: &now,
		Timestamp:   nil,
		Bool:        true,
		Char:        "A C",
		Varchar:     "12345678901234567890",
		Text:        "Very long text",
	}

	// Insert
	err := session.Insert(&in)
	if err != nil {
		t.Fatalf("error on Insert: %v", err)
	}
	if in.Id <= 0 {
		t.Errorf("expected Id to be > 0, got %d", in.Id)
	}

	// Load again
	qbe := struct{ Id int64 }{Id: in.Id}
	var out cruddy
	err = session.Find("select * from cruddy where id=:Id", qbe).Single(&out)
	if err != nil {
		t.Fatalf("error on Single: %v", err)
	}
	if out.Id != in.Id {
		t.Errorf("expected out.Id == %d, got %d", in.Id, out.Id)
	}
	if out.Int != in.Int {
		t.Errorf("expected out.Int == %d, got %d", in.Int, out.Int)
	}
}

// ---- Single --------------------------------------------------------------

func TestSingle(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		in := user{Id: 1}
		var out user
		err := session.Find("select * from users where id=:Id", in).Single(&out)
		if err != nil {
			t.Fatalf("error on Single: %v", err)
		}
		if out.Id != 1 {
			t.Errorf("expected user.Id == %d, got %d", 1, out.Id)
		}
		if out.Name != "Oliver" {
			t.Errorf("expected user.Name == %s, got %s", "Oliver", out.Name)
		}
		if out.Karma == nil {
			t.Errorf("expected user.Karma != nil, got %v", out.Karma)
		} else if *out.Karma != 42.13 {
			t.Errorf("expected user.Karma == %v, got %v", 42.13, *out.Karma)
		}
		if out.Suspended {
			t.Errorf("expected user.Suspended == %v, got %v", false, out.Suspended)
		}
	}
}

func TestSingleWithoutDataReturnsErrNoRows(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		in := user{Id: 42}
		var out user
		err := session.Find("select * from users where id=:Id", in).Single(&out)
		if err == nil {
			t.Fatalf("expected an error, got %v", err)
		}
		if err != sql.ErrNoRows {
			t.Errorf("expected error %v, got %v", sql.ErrNoRows, err)
		}
	}
}

func TestSingleIgnoresMissingColumns(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		qbe := Order{Id: 1}
		var out Order

		err := session.Find("select * from orders where id=:Id", qbe).Single(&out)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if out.Id != 1 {
			t.Errorf("expected order.Id == %d, got %d", 1, out.Id)
		}
	}
}

func TestSingleIgnoresAssociations(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		qbe := Order{Id: 1}
		var out Order

		err := session.Find("select * from orders where id=:Id", qbe).Single(&out)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if out.Id != 1 {
			t.Errorf("expected order.Id == %d, got %d", 1, out.Id)
		}
	}
}

func TestSingleWithProjection(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)

		in := user{Id: 1}
		var out user
		err := session.Find("select name from users where id=:Id", in).Single(&out)
		if err != nil {
			t.Fatalf("error on First: %v", err)
		}
		// Id should have its default value of 0, because it's not in the projection
		if out.Id != 0 {
			t.Errorf("expected user.Id == %d, got %d", 0, out.Id)
		}
		if out.Name != "Oliver" {
			t.Errorf("expected user.Name == %s, got %s", "Oliver", out.Name)
		}
		if out.Karma != nil {
			t.Errorf("expected user.Karma == nil, got %v", out.Karma)
		}
		if out.Suspended {
			t.Errorf("expected user.Suspended == %v, got %v", false, out.Suspended)
		}
	}
}

func TestSingleWithIncludes(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var order Order

		err := session.
			Find("select * from orders where id=1", nil).
			Include("Items").
			Single(&order)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if order.Id != 1 {
			t.Errorf("expected order.Id == %d, got %d", 1, order.Id)
		}
		if order.Items == nil {
			t.Fatalf("expected order items to be != nil")
		}
		if len(order.Items) != 2 {
			t.Errorf("expected len(order.Items) == %d, got %d", 2, len(order.Items))
		}
		for _, item := range order.Items {
			if item.OrderId != order.Id {
				t.Errorf("expected item.OrderId == order.Id, but %d != %d", item.OrderId, order.Id)
			}
		}
	}
}

// ---- All -----------------------------------------------------------------

func TestAll(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var results []user

		err := session.Find("select * from users order by id", nil).All(&results)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected len(results) == %d, got %d", 2, len(results))
		}
		for i, user := range results {
			if user.Id != int64(i+1) {
				t.Errorf("expected user to have id == %d, got %d", i+1, user.Id)
			}
			if user.Name == "" {
				t.Errorf("expected user to have Name != \"\", got %v", user.Name)
			}
			if user.Karma == nil {
				t.Errorf("expected user to have Karma != nil, got %v", user.Karma)
			}
		}
	}
}

func TestAllWithPtrToModel(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var results []*user

		err := session.Find("select * from users order by id", nil).All(&results)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected len(results) == %d, got %d", 2, len(results))
		}
		for i, user := range results {
			if user.Id != int64(i+1) {
				t.Errorf("expected user to have id == %d, got %d", i+1, user.Id)
			}
			if user.Name == "" {
				t.Errorf("expected user to have Name != \"\", got %v", user.Name)
			}
			if user.Karma == nil {
				t.Errorf("expected user to have Karma != nil, got %v", user.Karma)
			}
		}
	}
}

func TestAllWithProjections(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var results []user

		err := session.Find("select id,name from users order by name", nil).All(&results)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected len(results) == %d, got %d", 2, len(results))
		}
		for _, user := range results {
			if user.Id <= 0 {
				t.Errorf("expected user to have an Id > 0, got %d", user.Id)
			}
			// Column expected to be != ""
			if user.Name == "" {
				t.Errorf("expected user to have Name != \"\", got %v", user.Name)
			}
			// Karma is not in the projection, so it should have its default value
			if user.Karma != nil {
				t.Errorf("expected user to have Karma == nil, got %v", user.Karma)
			}
		}
	}
}

func TestAllIgnoresMissingColumns(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var results []userWithMissingColumns

		err := session.Find("select * from users order by name", nil).All(&results)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected len(results) == %d, got %d", 2, len(results))
		}
		for _, user := range results {
			if user.Id <= 0 {
				t.Errorf("expected user to have an Id > 0, got %d", user.Id)
			}
			// Column expected to be != ""
			if user.Name == "" {
				t.Errorf("expected user to have Name != \"\", got %v", user.Name)
			}
		}
	}
}

func TestAllIgnoresAssociations(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var results []Order

		err := session.Find("select * from orders", nil).All(&results)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if len(results) != 3 {
			t.Errorf("expected len(results) == %d, got %d", 3, len(results))
		}
	}
}

func TestAllWithOneToManyIncludes(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var orders []*Order

		err := session.
			// Debug(true).
			Find("select * from orders order by ref_id", nil).
			Include("Items").
			All(&orders)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if len(orders) != 3 {
			t.Errorf("expected len(orders) == %d, got %d", 3, len(orders))
		}
		for _, order := range orders {
			if order.Items == nil {
				t.Fatalf("expected order items to be != nil")
			}
			if order.Id == 1 && len(order.Items) != 2 {
				t.Errorf("expected len(order.Items) == %d, got %d", 2, len(order.Items))
			}
			if order.Id == 2 && len(order.Items) != 2 {
				t.Errorf("expected len(order.Items) == %d, got %d", 2, len(order.Items))
			}
			for _, item := range order.Items {
				if item.OrderId != order.Id {
					t.Errorf("expected item.OrderId == order.Id, but %d != %d", item.OrderId, order.Id)
				}
			}
		}
	}
}

func TestAllWithOneToOneIncludes(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var items []*OrderItem

		err := session.
			// Debug(true).
			Find("select * from order_items order by id", nil).
			Include("Order").
			All(&items)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if len(items) != 4 {
			t.Errorf("expected len(items) == %d, got %d", 4, len(items))
		}
		for _, item := range items {
			if item.Order == nil {
				t.Fatalf("expected item.Order to be != nil")
			}
			if item.OrderId != item.Order.Id {
				t.Errorf("expected item.OrderId == item.Order.Id, got %d != %d", item.OrderId, item.Order.Id)
			}
		}
	}
}

// ---- Scalar --------------------------------------------------------------

func TestScalarWithInt32(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var count int

		err := session.Find("select id from users where id=1", nil).Scalar(&count)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if count != 1 {
			t.Errorf("expected name to be %d, got %d", 1, count)
		}
	}
}

func TestScalarWithFloat(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var karma float32

		err := session.Find("select karma from users where id=1", nil).Scalar(&karma)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if karma != 42.13 {
			t.Errorf("expected name to be %v, got %v", 42.13, karma)
		}
	}
}

func TestScalarWithString(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var name string

		err := session.Find("select name from users where id=1", nil).Scalar(&name)
		if err != nil {
			t.Fatalf("error on Query: %v", err)
		}
		if name != "Oliver" {
			t.Errorf("expected name to be %s, got %s", "Oliver", name)
		}
	}
}

func TestScalarWithoutDataReturnsErrNoRows(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var name string

		err := session.Find("select name from users where id=42", nil).Scalar(&name)
		if err == nil {
			t.Fatalf("expected an error, got %v", err)
		}
		if err != sql.ErrNoRows {
			t.Errorf("expected error %v, got %v", sql.ErrNoRows, err)
		}
	}
}

// ---- Count ---------------------------------------------------------------

func TestCount(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)

		count, err := session.Count("select count(*) from users order by name", nil)
		if err != nil {
			t.Fatalf("driver %s: error on Query: %v", driver, err)
		}
		if count != 2 {
			t.Errorf("driver %s: expected count of users == %d, got %d", driver, 2, count)
		}
	}
}

func TestCountWithQueryParams(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)

		qbe := struct{ Id int64 }{1}

		count, err := session.Count("select count(*) from users where id=:Id", qbe)
		if err != nil {
			t.Fatalf("driver %s: error on Query: %v", driver, err)
		}
		if count != 1 {
			t.Errorf("driver %s: expected count of users == %d, got %d", driver, 1, count)
		}
	}
}

func TestCountWithWrongType(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)

		_, err := session.Count("select name from users order by name limit 1", nil)
		if err == nil {
			t.Fatalf("expected error, got %v", err)
		}
	}
}

// ---- Get -----------------------------------------------------------------

func TestGet(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var out Order
		err := session.Get(1).Do(&out)
		if err != nil {
			t.Fatalf("error on Get: %v", err)
		}
		if out.Id != 1 {
			t.Errorf("expected Order.Id == %d, got %d", 1, out.Id)
		}
		if out.RefId != "APPLE1" {
			t.Errorf("expected Order.RefId == %s, got %s", "APPLE1", out.RefId)
		}
	}
}

func TestGetWithNoSuchRow(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var out Order
		err := session.Get(987654321).Do(&out)
		if err != sql.ErrNoRows {
			t.Fatalf("expected error to be sql.ErrNoRows, got: %v", err)
		}
	}
}

func TestGetWithIncludeOfOneToMany(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var out Order
		err := session.Get(1).Include("Items").Do(&out)
		if err != nil {
			t.Fatalf("error on Get: %v", err)
		}
		if out.Id != 1 {
			t.Errorf("expected Id == %d, got %d", 1, out.Id)
		}
		if out.RefId != "APPLE1" {
			t.Errorf("expected RefId == %s, got %s", "APPLE1", out.RefId)
		}
		if len(out.Items) != 2 {
			t.Errorf("expected order to load 2 items, got %d items", len(out.Items))
		}
		for _, item := range out.Items {
			if item.OrderId != out.Id {
				t.Errorf("expected order item to reference order %d, got %d", out.Id, item.OrderId)
			}
		}
	}
}

func TestGetWithIncludeOfOneToOne(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)
		var out OrderItem
		err := session.Get(2).Include("Order").Do(&out)
		if err != nil {
			t.Fatalf("error on Get: %v", err)
		}
		if out.Id != 2 {
			t.Errorf("expected Id == %d, got %d", 2, out.Id)
		}
		if out.OrderId != 1 {
			t.Errorf("expected OrderId == %d, got %d", 1, out.OrderId)
		}
		if out.Order == nil {
			t.Fatalf("expected Order != nil")
		}
		if out.Order.Id != out.OrderId {
			t.Errorf("expected item.Order.Id == %d, got %d", 1, out.Order.Id)
		}
	}
}

// ---- Insert --------------------------------------------------------------

func TestInsert(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)

		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		k := float64(42.3)
		u := &user{
			Name:      "George",
			Karma:     &k,
			Suspended: false,
		}

		err := session.Insert(u)
		if err != nil {
			t.Fatalf("error on Insert: %v", err)
		}
		if u.Id <= 0 {
			t.Errorf("expected Id to be > 0, got %d", u.Id)
		}

		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount+1 {
			t.Errorf("expected users count to be %d, got %d", oldCount+1, newCount)
		}
	}
}

func TestInsertWithoutTableNameTagFails(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)

		k := float64(42.3)
		u := &userWithoutTableNameTag{
			Name:      "George",
			Karma:     &k,
			Suspended: false,
		}

		err := session.Insert(u)
		if err != ErrNoTableName {
			t.Fatalf("expected dapper.ErrNoTableName, got: %v", err)
		}
	}
}

func TestInsertTx(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)

		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		k := float64(42.3)
		u := &user{
			Name:      "George",
			Karma:     &k,
			Suspended: false,
		}

		// Begin transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("error on db.Begin(): %v", err)
		}

		// Insert
		err = session.InsertTx(tx, u)
		if err != nil {
			t.Fatalf("error on InsertTx: %v", err)
		}
		if u.Id <= 0 {
			t.Errorf("expected Id to be > 0, got %d", u.Id)
		}

		// Commit transaction
		err = tx.Commit()
		if err != nil {
			t.Fatalf("error on Commit: %v", err)
		}

		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount+1 {
			t.Errorf("expected users count to be %d, got %d", oldCount+1, newCount)
		}
	}
}

func TestInsertTxWithRollback(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		session := New(db)

		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		k := float64(42.3)
		u := &user{
			Name:      "George",
			Karma:     &k,
			Suspended: false,
		}

		// Begin transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("error on db.Begin(): %v", err)
		}

		// Insert
		err = session.InsertTx(tx, u)
		if err != nil {
			t.Fatalf("error on InsertTx: %v", err)
		}
		if u.Id <= 0 {
			t.Errorf("expected Id to be > 0, got %d", u.Id)
		}

		// Rollback transaction
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("error on Rollback: %v", err)
		}

		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount {
			t.Errorf("expected users count to be %d, got %d", oldCount, newCount)
		}
	}
}

// ---- Update --------------------------------------------------------------

func TestUpdate(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		// Count users
		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		// Retrieve user
		session := New(db)
		var u user
		err := session.Find("select * from users where id=1", nil).Single(&u)
		if err != nil {
			t.Fatalf("error on find single: %v", err)
		}

		// Change user
		u.Name = "Olli"

		// Update user
		err = session.Update(u)
		if err != nil {
			t.Fatalf("error on Update: %v", err)
		}

		// Reload user
		var u2 user
		session.Find("select * from users where id=1", nil).Single(&u2)
		if u2.Name != u.Name {
			t.Errorf("expected user name to be %s, got %s", u.Name, u2.Name)
		}

		// Check count again
		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount {
			t.Errorf("expected users count to be %d, got %d", oldCount, newCount)
		}
	}
}

func TestUpdateWithPtrType(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		// Count users
		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		// Retrieve user
		session := New(db)
		var u user
		err := session.Find("select * from users where id=1", nil).Single(&u)
		if err != nil {
			t.Fatalf("error on find single: %v", err)
		}

		// Change user
		u.Name = "Olli"

		// Update user
		err = session.Update(&u)
		if err != nil {
			t.Fatalf("error on Update: %v", err)
		}

		// Reload user
		var u2 user
		session.Find("select * from users where id=1", nil).Single(&u2)
		if u2.Name != u.Name {
			t.Errorf("expected user name to be %s, got %s", u.Name, u2.Name)
		}

		// Check count again
		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount {
			t.Errorf("expected users count to be %d, got %d", oldCount, newCount)
		}
	}
}

func TestUpdateWithoutPrimaryKeyTagFails(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		// Retrieve user
		session := New(db)
		var u userWithoutPrimaryKeyTag
		err := session.Find("select * from users where id=1", nil).Single(&u)

		u.Name = "Olli"

		err = session.Update(u)
		if err != ErrNoPrimaryKey {
			t.Fatalf("expected dapper.ErrNoPrimaryKey, got: %v", err)
		}
	}
}

func TestUpdateTx(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		// Count users
		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		// Retrieve user
		session := New(db)
		var u user
		err := session.Find("select * from users where id=1", nil).Single(&u)
		if err != nil {
			t.Fatalf("error on find single: %v", err)
		}

		// Change user
		u.Name = "Olli"

		// Start transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("error on db.Begin(): %v", err)
		}

		// Update user
		err = session.UpdateTx(tx, u)
		if err != nil {
			t.Fatalf("error on UpdateTx: %v", err)
		}

		// Commit
		err = tx.Commit()
		if err != nil {
			t.Fatalf("error on db.Commit(): %v", err)
		}

		// Reload user
		var u2 user
		session.Find("select * from users where id=1", nil).Single(&u2)
		if u2.Name != u.Name {
			t.Errorf("expected user name to be %s, got %s", u.Name, u2.Name)
		}

		// Check count again
		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount {
			t.Errorf("expected users count to be %d, got %d", oldCount, newCount)
		}
	}
}

func TestUpdateTxRollback(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		// Count users
		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		// Retrieve user
		session := New(db)
		var u user
		err := session.Find("select * from users where id=1", nil).Single(&u)
		if err != nil {
			t.Fatalf("error on find single: %v", err)
		}

		// Start transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("error on db.Begin(): %v", err)
		}

		// Change user
		u.Name = "Olli"

		// Update user
		err = session.UpdateTx(tx, u)
		if err != nil {
			t.Fatalf("error on UpdateTx: %v", err)
		}

		// Rollback transaction
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("error on Rollback: %v", err)
		}

		// Reload user
		var u2 user
		session.Find("select * from users where id=1", nil).Single(&u2)
		if u2.Name == u.Name {
			t.Errorf("expected user name to be %s, got %s", u.Name, u2.Name)
		}

		// Check count again
		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount {
			t.Errorf("expected users count to be %d, got %d", oldCount, newCount)
		}
	}
}

// ---- Delete --------------------------------------------------------------

func TestDelete(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		// Count users
		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		// Retrieve user
		session := New(db)
		var u user
		err := session.Find("select * from users where id=1", nil).Single(&u)
		if err != nil {
			t.Fatalf("error on find single: %v", err)
		}

		// Delete user
		err = session.Delete(u)
		if err != nil {
			t.Fatalf("error on Delete: %v", err)
		}

		// Check count
		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount-1 {
			t.Errorf("expected users count to be %d, got %d", oldCount-1, newCount)
		}
	}
}

func TestDeleteWithPtrType(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		// Count users
		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		// Retrieve user
		session := New(db)
		var u user
		err := session.Find("select * from users where id=1", nil).Single(&u)
		if err != nil {
			t.Fatalf("error on find single: %v", err)
		}

		// Delete user
		err = session.Delete(&u)
		if err != nil {
			t.Fatalf("error on Delete: %v", err)
		}

		// Check count
		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount-1 {
			t.Errorf("expected users count to be %d, got %d", oldCount-1, newCount)
		}
	}
}

func TestDeleteTx(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		// Count users
		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		// Retrieve user
		session := New(db)
		var u user
		err := session.Find("select * from users where id=1", nil).Single(&u)
		if err != nil {
			t.Fatalf("error on find single: %v", err)
		}

		// Get a transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("error on db.Begin(): %v", err)
		}

		// Delete user
		err = session.DeleteTx(tx, u)
		if err != nil {
			t.Fatalf("error on Delete: %v", err)
		}

		// Commit
		err = tx.Commit()
		if err != nil {
			t.Fatalf("error on db.Commit(): %v", err)
		}

		// Check count
		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount-1 {
			t.Errorf("expected users count to be %d, got %d", oldCount-1, newCount)
		}
	}
}

func TestDeleteTxRollback(t *testing.T) {
	for _, driver := range drivers {
		db := setup(driver, t)
		defer db.Close()

		// Count users
		var oldCount int64
		row := db.QueryRow("select count(*) from users")
		row.Scan(&oldCount)

		// Retrieve user
		session := New(db)
		var u user
		err := session.Find("select * from users where id=1", nil).Single(&u)
		if err != nil {
			t.Fatalf("error on find single: %v", err)
		}

		// Get a transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("error on db.Begin(): %v", err)
		}

		// Delete user
		err = session.DeleteTx(tx, u)
		if err != nil {
			t.Fatalf("error on Delete: %v", err)
		}

		// Rollback
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("error on db.Rollback(): %v", err)
		}

		// Check count
		var newCount int64
		row = db.QueryRow("select count(*) from users")
		row.Scan(&newCount)

		if newCount != oldCount {
			t.Errorf("expected users count to be %d, got %d", oldCount, newCount)
		}
	}
}
