# Dapper for Go

This is a simple object mapper for Google Go.

It is based on the idea of [Dapper](https://github.com/SamSaffron/dapper-dot-net).

Is fairly limited, e.g. query only. But if you can use it (or like it to
implement your own simple object mapper), here it is.

## Status

This is still a work in progress. Use at your own risk.

## Installation

Install via `go get github.com/olivere/dapper`.

## Concepts

Dapper has two concepts: Generating SQL statements (limited to MySQL)
and running SQL statements and returning results in Go structs.

## SQL generation

Maybe you're like me and want your program to generate a SQL statement
based on some conditions. While you can build SQL manually, that's rather
cumbersome.

I'd rather do something like this:

    sql := dapper.Q("users").Alias("u").
        Join("tweets").Alias("t").On("u.id", "t.user_id").
        Project("u.name", "t.message").
        Order().Asc("u.name").
        Order().Desc("t.created").
        Take(10).
        Sql()

    => SELECT u.name,t.message 
         FROM users u 
         JOIN tweets t ON u.id=t.user_id
        ORDER BY u.name ASC,t.created DESC
        LIMIT 10

## Querying

You can use the SQL generation as input for querying, or you create the
SQL manually. Either way, querying works as follows.

First, specify the "entities" and apply tags to let Dapper find
a mapping between the struct field and the database column:

    type User struct {
        Id        int64      `dapper:"id,primarykey,autoincrement"`
        Name      string     `dapper:"name"`
        Country   string     `dapper:"country"`
        Karma     *float64   `dapper:"karma"`
        Suspended bool       `dapper:"suspended"`
        Ignored   string     `dapper:"-"`
    }

Of course, you need to connect to a database and get yourself a `*sql.DB`:

    db, err := sql.Open(...)

Then create a Dapper session:

    session := dapper.New(db)

Now you can throw some SQL at Dapper and let it fill your result set:

    // Build SQL statement (or use a manually crafted SQL string)
    sql := dapper.Q("users").Alias("u").
        Join("tweets").Alias("t").On("u.id", "t.user_id").
        Project("u.name", "t.message").
        Order().Asc("u.name").
        Order().Desc("t.created").
        Take(10).
        Sql()

    // Run the query and return all users in a slice
    var users []User
    err := session.Find(sql, nil).All(&users)
    if err != nil {
        // ...
    }

    // Iterate
    for _, user := range users {
        fmt.Println(user.Name)
    }

But there's a second way of executing SQL queries. You can use with a 
struct that serves as a binding to the query. Here's how:

    // A binding with a UserId field in it
    type UserByIdQuery struct {
        UserId    int64
    }

    // Another binding with some more fields
    type UserbyComplexQuery struct {
        MinKarma  float64
        MaxKarma  float64
        Country   string
    }

To get the first result of a query:

    // Reserve a User variable for the result
    var user User

    // Fill the binding for the query: UserId=1
    queryParam := UserByIdQuery{UserId: 1}

    // Now run the query:
    // Notice the ":UserId" will be retrieved from the binding, so the
    // SQL statement is: "select * from users where id=1"
    err := session.Find("select * from users where id=:UserId", queryParam).Single(&user)
    if err != nil {
    	// ...
    }

To perform a query returning not a single entity but a slice:

    // Another binding
    queryParam := UsersByComplexQuery{MinKarma: 17.0, Country: "DE"}

    // Retrieve the results
    var results []User
    err := session.Find("select * from users "+
        "where karma > :MinKarma and country = :Country "+
        "order by name limit 30", queryParam).All(&results)
    if err != nil {
        // ...
    }

    // Iterate
    for _, user := range users {
        fmt.Println(user.Name)
    }

You can also retrieve the first column of the first row by using the
Scalar function:

    // Create a var for the result
    var name string 

    // Stores the user name
    err := session.Find("select name from users where id=1", nil).Scalar(&name)

As counting is a very common operating, there is a shortcut:

    // Returns the number of users
	  count, err := session.Count("select count(*) from users", nil)

    
## Credits

* [Sam Saffron](http://www.samsaffron.com/) for Dapper.

## License

MIT LICENSE. See [LICENSE](http://olivere.mit-license.org/) or the
LICENSE file in the repository.
