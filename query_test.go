package dapper

import (
	"testing"
)

func TestMySQLSimpleQueries(t *testing.T) {
	sql := Q(MySQL, "users").Sql()
	if sql != "SELECT * FROM users" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users", sql)
	}

	sql = Q(MySQL, "users").Where().Eq("id", 1).Sql()
	if sql != "SELECT * FROM users WHERE id=1" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE id=1", sql)
	}

	sql = Q(MySQL, "users").Where().Eq("name", "oliver").Sql()
	if sql != "SELECT * FROM users WHERE name='oliver'" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE name='oliver'", sql)
	}

	sql = Q(MySQL, "users").Where().Eq("name", "mc'alister").Sql()
	if sql != "SELECT * FROM users WHERE name='mc\\'alister'" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE name='mc\\'alister'", sql)
	}

	sql = Q(MySQL, "users").Where().Eq("expired", nil).Sql()
	if sql != "SELECT * FROM users WHERE expired IS NULL" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE expired IS NULL", sql)
	}

	sql = Q(MySQL, "users").Where().EqCol("expired", "expired2").Sql()
	if sql != "SELECT * FROM users WHERE expired=expired2" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE expired=expired2", sql)
	}

	sql = Q(MySQL, "users").Where().Ne("id", 1).Sql()
	if sql != "SELECT * FROM users WHERE id<>1" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE id<>1", sql)
	}

	sql = Q(MySQL, "users").Where().Ne("expired", nil).Sql()
	if sql != "SELECT * FROM users WHERE expired IS NOT NULL" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE expired IS NOT NULL", sql)
	}

	sql = Q(MySQL, "users").Where().NeCol("expired", "expired2").Sql()
	if sql != "SELECT * FROM users WHERE expired<>expired2" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE expired<>expired2", sql)
	}

	sql = Q(MySQL, "users").Where().In("id", 1, 2, 3, 4).Sql()
	if sql != "SELECT * FROM users WHERE id IN (1,2,3,4)" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE id IN (1,2,3,4)", sql)
	}

	sql = Q(MySQL, "users").Where().NotIn("id", 1, 2, 3, 4).Sql()
	if sql != "SELECT * FROM users WHERE id NOT IN (1,2,3,4)" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users WHERE id NOT IN (1,2,3,4)", sql)
	}
}

func TestMySQLSubQueries(t *testing.T) {
	// Subquery with numerical columns
	subQ := Q(MySQL, "tweets").
		Project("count(tweets.id)").
		Where().
		EqCol("tweets.user_id", "users.user_id").
		Eq("tweets.retweets", 25).
		Query()
	sql := Q(MySQL, "users").
		Project("users.*, " +
		"(" + subQ.Sql() + ") num_tweets").
		Sql()
	expected := "SELECT users.*, (SELECT count(tweets.id) FROM tweets WHERE tweets.user_id=users.user_id AND tweets.retweets=25) num_tweets FROM users"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}

	// Subquery with string columns
	subQ = Q(MySQL, "tweets").
		Project("count(tweets.id)").
		Where().
		EqCol("tweets.user_id", "users.user_id").
		Eq("tweets.message", "Hello").
		Query()

	//t.Logf("subQ: %s", subQ.Sql())

	sql = Q(MySQL, "users").
		Project("users.*", SafeSqlString("("+subQ.Sql()+") num_tweets")).Sql()
	expected = "SELECT users.*,(SELECT count(tweets.id) FROM tweets WHERE tweets.user_id=users.user_id AND tweets.message='Hello') num_tweets FROM users"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryProjection(t *testing.T) {
	sql := Q(MySQL, "users").
		Project("name").
		Sql()
	if sql != "SELECT name FROM users" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users", sql)
	}

	sql = Q(MySQL, "users").
		Where().Eq("users.id", 2).
		Project("users.name").
		Sql()
	if sql != "SELECT users.name FROM users WHERE users.id=2" {
		t.Errorf("expected %v, got %v", "SELECT users.name FROM users WHERE users.id=2", sql)
	}
}

func TestMySQLSafeSqlString(t *testing.T) {
	safeSql := SafeSqlString("don't escape me")
	sql := Q(MySQL, "users").
		Project("name", safeSql).
		Sql()
	expected := "SELECT name,don't escape me FROM users"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLChainedQueries(t *testing.T) {
	q := Q(MySQL, "users").Where().Eq("id", 1).Query()
	q = q.Where().Eq("name", "Oliver").Query()
	got := q.Sql()
	expected := "SELECT * FROM users WHERE id=1 AND name='Oliver'"
	if got != expected {
		t.Errorf("expected %v, got %v", expected, got)
	}
}

func TestMySQLQueryWithLimits(t *testing.T) {
	sql := Q(MySQL, "users").Take(10).Sql()
	if sql != "SELECT * FROM users LIMIT 10" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users LIMIT 10", sql)
	}

	sql = Q(MySQL, "users").Skip(20).Sql()
	if sql != "SELECT * FROM users LIMIT 20,0" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users LIMIT 20,0", sql)
	}

	sql = Q(MySQL, "users").Skip(20).Take(10).Sql()
	if sql != "SELECT * FROM users LIMIT 20,10" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users LIMIT 20,10", sql)
	}
}

func TestMySQLQueryJoins(t *testing.T) {
	sql := Q(MySQL, "users").
		Join("tweets").On("users.id", "tweets.user_id").
		Sql()
	if sql != "SELECT * FROM users JOIN tweets ON users.id=tweets.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users JOIN tweets ON users.id=tweets.user_id", sql)
	}

	sql = Q(MySQL, "users").Alias("u").
		Join("tweets").Alias("t").On("u.id", "t.user_id").
		Take(10).
		Sql()
	if sql != "SELECT * FROM users u JOIN tweets t ON u.id=t.user_id LIMIT 10" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users u JOIN tweets t ON u.id=t.user_id LIMIT 10", sql)
	}

	sql = Q(MySQL, "users").
		Join("tweets").On("users.id", "tweets.user_id").
		Join("followers").On("followers.follower_id", "users.user_id").
		Sql()
	if sql != "SELECT * FROM users JOIN tweets ON users.id=tweets.user_id JOIN followers ON followers.follower_id=users.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users JOIN tweets ON users.id=tweets.user_id JOIN followers ON followers.follower_id=users.user_id", sql)
	}
}

func TestMySQLInnerJoins(t *testing.T) {
	sql := Q(MySQL, "users").
		InnerJoin("tweets").On("users.id", "tweets.user_id").
		Sql()
	if sql != "SELECT * FROM users INNER JOIN tweets ON users.id=tweets.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users INNER JOIN tweets ON users.id=tweets.user_id", sql)
	}
}

func TestMySQLLeftInnerJoins(t *testing.T) {
	sql := Q(MySQL, "users").
		LeftInnerJoin("tweets").On("users.id", "tweets.user_id").
		Sql()
	if sql != "SELECT * FROM users LEFT INNER JOIN tweets ON users.id=tweets.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users LEFT INNER JOIN tweets ON users.id=tweets.user_id", sql)
	}
}

func TestMySQLOuterJoins(t *testing.T) {
	sql := Q(MySQL, "users").
		OuterJoin("tweets").On("users.id", "tweets.user_id").
		Sql()
	if sql != "SELECT * FROM users OUTER JOIN tweets ON users.id=tweets.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users OUTER JOIN tweets ON users.id=tweets.user_id", sql)
	}
}

func TestMySQLLeftOuterJoins(t *testing.T) {
	sql := Q(MySQL, "users").
		LeftOuterJoin("tweets").On("users.id", "tweets.user_id").
		Sql()
	if sql != "SELECT * FROM users LEFT OUTER JOIN tweets ON users.id=tweets.user_id" {
		t.Errorf("expected %v, got %v", "SELECT * FROM users LEFT OUTER JOIN tweets ON users.id=tweets.user_id", sql)
	}
}

func TestMySQLComplexQuery(t *testing.T) {
	sql := Q(MySQL, "users").Alias("u").
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

func TestMySQLQueryEqualColumn(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().EqCol("message", "user").
		Sql()

	expected := "SELECT * FROM tweets WHERE message=user"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryNotEqualColumn(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().NeCol("message", "user").
		Sql()

	expected := "SELECT * FROM tweets WHERE message<>user"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryEqual(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().Eq("message", "Google").
		Sql()

	expected := "SELECT * FROM tweets WHERE message='Google'"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryEqualWithSafeString(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().Eq("message", SafeSqlString("'don't escape me'")).
		Sql()

	expected := "SELECT * FROM tweets WHERE message='don't escape me'"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryNotEqual(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().Ne("message", "Google").
		Sql()

	expected := "SELECT * FROM tweets WHERE message<>'Google'"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryNotEqualWithSafeString(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().Ne("message", SafeSqlString("'don't escape me'")).
		Sql()

	expected := "SELECT * FROM tweets WHERE message<>'don't escape me'"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryLike(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().Like("message", "%Google%").
		Sql()

	expected := "SELECT * FROM tweets WHERE message LIKE '%Google%'"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryLikeWithSafeSqlString(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().Like("message", SafeSqlString("'%don't escape me%'")).
		Sql()

	expected := "SELECT * FROM tweets WHERE message LIKE '%don't escape me%'"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryNotLike(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().NotLike("message", "%Google%").
		Sql()

	expected := "SELECT * FROM tweets WHERE message NOT LIKE '%Google%'"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryNotLikeWithSafeSqlString(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().NotLike("message", SafeSqlString("'%don't escape me%'")).
		Sql()

	expected := "SELECT * FROM tweets WHERE message NOT LIKE '%don't escape me%'"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryInClause(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().In("id", 1, 2).
		Sql()

	expected := "SELECT * FROM tweets WHERE id IN (1,2)"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryInClauseWithSafeString(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().In("id", 1, 2, SafeSqlString("Oops")).
		Sql()

	expected := "SELECT * FROM tweets WHERE id IN (1,2,Oops)"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryInClauseAsArray(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().In("id", []int{1, 2}).
		Sql()

	expected := "SELECT * FROM tweets WHERE id IN (1,2)"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryNotInClause(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().NotIn("id", 1, 2).
		Sql()

	expected := "SELECT * FROM tweets WHERE id NOT IN (1,2)"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryNotInClauseWithSafeString(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().NotIn("id", 1, 2, SafeSqlString("Ooops")).
		Sql()

	expected := "SELECT * FROM tweets WHERE id NOT IN (1,2,Ooops)"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}

func TestMySQLQueryNotInClauseAsArray(t *testing.T) {
	sql := Q(MySQL, "tweets").
		Where().NotIn("id", []int{1, 2}).
		Sql()

	expected := "SELECT * FROM tweets WHERE id NOT IN (1,2)"
	if sql != expected {
		t.Errorf("expected %v, got %v", expected, sql)
	}
}
