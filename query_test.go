package dapper

import (
	"testing"
)

func TestSimpleQueries(t *testing.T) {
	sql := Q("users").Sql()
	if sql != "SELECT * FROM users" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users", sql)
	}

	sql = Q("users").Where().Eq("id", 1).Sql()
	if sql != "SELECT * FROM users WHERE id=1" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE id=1", sql)
	}

	sql = Q("users").Where().Eq("expired", nil).Sql()
	if sql != "SELECT * FROM users WHERE expired IS NULL" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE expired IS NULL", sql)
	}

	sql = Q("users").Where().EqCol("expired", "expired2").Sql()
	if sql != "SELECT * FROM users WHERE expired=expired2" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE expired=expired2", sql)
	}

	sql = Q("users").Where().Ne("id", 1).Sql()
	if sql != "SELECT * FROM users WHERE id<>1" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE id<>1", sql)
	}

	sql = Q("users").Where().Ne("expired", nil).Sql()
	if sql != "SELECT * FROM users WHERE expired IS NOT NULL" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE expired IS NOT NULL", sql)
	}

	sql = Q("users").Where().NeCol("expired", "expired2").Sql()
	if sql != "SELECT * FROM users WHERE expired<>expired2" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE expired<>expired2", sql)
	}
}

func TestQueryProjection(t *testing.T) {
	sql := Q("users").
		Project("name").
		Sql()
	if sql != "SELECT name FROM users" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users", sql)
	}

	sql = Q("users").
		Where().Eq("users.id", 2).
		Project("users.name").
		Sql()
	if sql != "SELECT users.name FROM users WHERE users.id=2" {
		t.Errorf("expected %v, got %v", "SELECT users.name FROM users WHERE users.id=2", sql)
	}
}

func TestQueryWithLimits(t *testing.T) {
	sql := Q("users").Take(10).Sql()
	if sql != "SELECT * FROM users LIMIT 10" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users LIMIT 10", sql)
	}

	sql = Q("users").Skip(20).Sql()
	if sql != "SELECT * FROM users LIMIT 20,0" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users LIMIT 20,0", sql)
	}

	sql = Q("users").Skip(20).Take(10).Sql()
	if sql != "SELECT * FROM users LIMIT 20,10" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users LIMIT 20,10", sql)
	}
}

func TestQueryJoins(t *testing.T) {
	sql := Q("users").
		Join("tweets").On("users.id", "tweets.user_id").
		Sql()
	if sql != "SELECT * FROM users JOIN tweets ON users.id=tweets.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users JOIN tweets ON users.id=tweets.user_id", sql)
	}

	sql = Q("users").Alias("u").
		Join("tweets").Alias("t").On("u.id", "t.user_id").
		Take(10).
		Sql()
	if sql != "SELECT * FROM users u JOIN tweets t ON u.id=t.user_id LIMIT 10" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users u JOIN tweets t ON u.id=t.user_id LIMIT 10", sql)
	}

	sql = Q("users").
		Join("tweets").On("users.id", "tweets.user_id").
		Join("followers").On("followers.follower_id", "users.user_id").
		Sql()
	if sql != "SELECT * FROM users JOIN tweets ON users.id=tweets.user_id JOIN followers ON followers.follower_id=users.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users JOIN tweets ON users.id=tweets.user_id JOIN followers ON followers.follower_id=users.user_id", sql)
	}
}

func TestInnerJoins(t *testing.T) {
	sql := Q("users").
		InnerJoin("tweets").On("users.id", "tweets.user_id").
		Sql()
	if sql != "SELECT * FROM users INNER JOIN tweets ON users.id=tweets.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users INNER JOIN tweets ON users.id=tweets.user_id", sql)
	}
}

func TestLeftInnerJoins(t *testing.T) {
	sql := Q("users").
		LeftInnerJoin("tweets").On("users.id", "tweets.user_id").
		Sql()
	if sql != "SELECT * FROM users LEFT INNER JOIN tweets ON users.id=tweets.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users LEFT INNER JOIN tweets ON users.id=tweets.user_id", sql)
	}
}

func TestOuterJoins(t *testing.T) {
	sql := Q("users").
		OuterJoin("tweets").On("users.id", "tweets.user_id").
		Sql()
	if sql != "SELECT * FROM users OUTER JOIN tweets ON users.id=tweets.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users OUTER JOIN tweets ON users.id=tweets.user_id", sql)
	}
}

func TestLeftOuterJoins(t *testing.T) {
	sql := Q("users").
		LeftOuterJoin("tweets").On("users.id", "tweets.user_id").
		Sql()
	if sql != "SELECT * FROM users LEFT OUTER JOIN tweets ON users.id=tweets.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users LEFT OUTER JOIN tweets ON users.id=tweets.user_id", sql)
	}
}

func TestComplexQuery(t *testing.T) {
	sql := Q("users").Alias("u").
		Join("tweets").Alias("t").On("u.id", "t.user_id").
		Project("u.name", "t.message").
		Order().Asc("u.name").
		Order().Desc("t.created").
		Take(10).
		Sql()

	expected := "SELECT u.name,t.message FROM users u JOIN tweets t ON u.id=t.user_id ORDER BY u.name ASC,t.created DESC LIMIT 10"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestQueryLike(t *testing.T) {
	sql := Q("tweets").
		Where().Like("message", "%Google%").
		Sql()

	expected := "SELECT * FROM tweets WHERE message LIKE '%Google%'"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestQueryNotLike(t *testing.T) {
	sql := Q("tweets").
		Where().NotLike("message", "%Google%").
		Sql()

	expected := "SELECT * FROM tweets WHERE message NOT LIKE '%Google%'"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestQueryInClause(t *testing.T) {
	sql := Q("tweets").
		Where().In("id", 1, 2).
		Sql()

	expected := "SELECT * FROM tweets WHERE id IN (1,2)"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestQueryInClauseAsArray(t *testing.T) {
	sql := Q("tweets").
		Where().In("id", []int{1, 2}).
		Sql()

	expected := "SELECT * FROM tweets WHERE id IN (1,2)"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestQueryNotInClause(t *testing.T) {
	sql := Q("tweets").
		Where().NotIn("id", 1, 2).
		Sql()

	expected := "SELECT * FROM tweets WHERE id NOT IN (1,2)"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestQueryNotInClauseAsArray(t *testing.T) {
	sql := Q("tweets").
		Where().NotIn("id", []int{1, 2}).
		Sql()

	expected := "SELECT * FROM tweets WHERE id NOT IN (1,2)"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}
