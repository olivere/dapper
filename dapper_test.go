package dapper

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	_ "github.com/ziutek/mymysql/godrv"
)

const (
	testDBName = "dapper_test"
	testDBUser = "dapper"
	testDBPass = "dapper"
)

type tweet struct {
	Id       int64     `dapper:"id,primarykey,serial"`
	UserId   int64     `dapper:"user_id"`
	Message  string    `dapper:"message"`
	Retweets int64     `dapper:"retweets"`
	Created  time.Time `dapper:"created"`
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
	Id          int64 `dapper:"id,primarykey,autoincrement"`
	Ignore      string `dapper:"-"`
	UserId      int64
}

func (t *tweet) String() string {
	return fmt.Sprintf("tweet[Id=%v,UserId=%v,Message=%v,Retweets=%v,Created=%v]",
		t.Id, t.UserId, t.Message, t.Retweets, t.Created)
}

type user struct {
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
	Id        int64    `dapper:"id,primarykey,autoincrement,table=users"`
	Name      string   `dapper:"name"`
}

func (u *user) String() string {
	return fmt.Sprintf("user[Id=%v,Name=%v,Karma=%v,Suspended=%v]",
		u.Id, u.Name, u.Karma, u.Suspended)
}

func setup(t *testing.T) *sql.DB {
	connectionString := fmt.Sprintf("%s/%s/%s", testDBName, testDBUser, testDBPass)
	db, err := sql.Open("mymysql", connectionString)
	if err != nil {
		t.Fatalf("error connection to database: %v", err)
	}

	// Drop tables
	_, err = db.Exec("DROP TABLE IF EXISTS tweets")
	if err != nil {
		t.Fatalf("error dropping tweets table: %v", err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Fatalf("error dropping users table: %v", err)
	}

	// Create tables
	_, err = db.Exec(`
CREATE TABLE users (
        id int(11) not null auto_increment,
        name varchar(100) not null,
        karma decimal(19,5),
        suspended tinyint(1) default '0',
        primary key (id)
)`)
	if err != nil {
		t.Fatalf("error creating users table: %v", err)
	}

	_, err = db.Exec(`
CREATE TABLE tweets (
        id int(11) not null auto_increment,
        user_id int(11) not null,
        message text,
        retweets int,
        created timestamp not null default current_timestamp,
        primary key (id),
        foreign key (user_id) references users (id) on delete cascade
)`)
	if err != nil {
		t.Fatalf("error creating tweets table: %v", err)
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

	return db
}

func TestTypeCache(t *testing.T) {
	db := setup(t)
	defer db.Close()

	if len(typeCache) != 0 {
		t.Errorf("expected type cache to be empty, got %d entries", len(typeCache))
	}

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

func TestSingle(t *testing.T) {
	db := setup(t)
	defer db.Close()

	session := New(db)
	in := user{Id: 1}
	var out user
	err := session.Find("select * from users where id=:Id", in).Single(&out)
	if err != nil {
		t.Fatalf("error on First: %v", err)
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

func TestSingleWithoutDataReturnsErrNoRows(t *testing.T) {
	db := setup(t)
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

func TestSingleWithProjection(t *testing.T) {
	db := setup(t)
	defer db.Close()

	session := New(db)

	in := user{Id: 1}
	var out user
	err := session.Find("select name from users where id=:Id", in).Single(&out)
	if err != nil {
		t.Fatalf("error on First: %v", err)
	}
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

func TestAll(t *testing.T) {
	db := setup(t)
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

func TestAllWithPtrToModel(t *testing.T) {
	db := setup(t)
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

func TestAllWithProjections(t *testing.T) {
	db := setup(t)
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

func TestAllIgnoresMissingColumns(t *testing.T) {
	db := setup(t)
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

func TestScalarWithInt32(t *testing.T) {
	db := setup(t)
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

func TestScalarWithFloat(t *testing.T) {
	db := setup(t)
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

func TestScalarWithString(t *testing.T) {
	db := setup(t)
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

func TestScalarWithoutDataReturnsErrNoRows(t *testing.T) {
	db := setup(t)
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

func TestCount(t *testing.T) {
	db := setup(t)
	defer db.Close()

	session := New(db)

	count, err := session.Count("select count(*) from users order by name", nil)
	if err != nil {
		t.Fatalf("error on Query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected count of users == %d, got %d", 2, count)
	}
}

func TestCountWithWrongType(t *testing.T) {
	db := setup(t)
	defer db.Close()

	session := New(db)

	_, err := session.Count("select name from users order by name limit 1", nil)
	if err != ErrWrongType {
		t.Fatalf("expected ErrWrongType as error, got %v", err)
	}
}

func TestInsert(t *testing.T) {
	db := setup(t)
	defer db.Close()

	session := New(db)

	var oldCount int64
	row := db.QueryRow("select count(*) from users")
	row.Scan(&oldCount)

	k := float64(42.3)
	u := &user{
		Name: "George",
		Karma: &k,
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

func TestInsertWithoutTableNameTagFails(t *testing.T) {
	db := setup(t)
	defer db.Close()

	session := New(db)

	k := float64(42.3)
	u := &userWithoutTableNameTag{
		Name: "George",
		Karma: &k,
		Suspended: false,
	}

	err := session.Insert(u)
	if err != ErrNoTableName {
		t.Fatalf("expected dapper.ErrNoTableName, got: %v", err)
	}
}

func TestUpdate(t *testing.T) {
	db := setup(t)
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

func TestUpdateWithPtrType(t *testing.T) {
	db := setup(t)
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

func TestUpdateWithoutPrimaryKeyTagFails(t *testing.T) {
	db := setup(t)
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

func TestDelete(t *testing.T) {
	db := setup(t)
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

func TestDeleteWithPtrType(t *testing.T) {
	db := setup(t)
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
