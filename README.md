# Dapper for Go

This is a simple object mapper for Google Go.

It is based on the idea of [Dapper](https://github.com/SamSaffron/dapper-dot-net).

Is fairly limited, e.g. query only. But if you can use it (or like it to
implement your own simple object mapper), here it is.

## Status

This is still a work in progress. Use at your own risk.

## Usage

Install via `go get github.com/olivere/dapper`.

Specify queries and entities:

    type User struct {
        Id        int64      `dapper:"id,primarykey,autoincrement"`
        Name      string     `dapper:"name"`
        Country   string     `dapper:"country"`
        Karma     *float64   `dapper:"karma"`
        Suspended bool       `dapper:"suspended"`
        Ignored   string     `dapper:"-"`
    }

    type UserByIdQuery struct {
        UserId    int64
    }

    type UserbyComplexQuery struct {
        MinKarma  float64
        MaxKarma  float64
        Country   string
    }

In Go, connect to a database and get yourself a `*sql.DB`:

    db, err := sql.Open(...)

To get the first result of a query:

    queryParam := UserByIdQuery{UserId: 1}
    user := User{}
    err := dapper.First(db, "select * from users where id=:UserId", queryParam, &user)
    if err != nil {
    	// ...
    }

To perform a query:

    queryParam := UsersByComplexQuery{MinKarma: 17.0, Country: "DE"}
    results, err := dapper.Query("select * from users "+
        "where karma > :MinKarma and country = :Country "+
        "order by name limit 30", queryParam, reflect.TypeOf(User{}))
    if err != nil {
        // ...
    }
    for _, result := range results {
        user, ok := result.(*User)
        if !ok { ... }
        fmt.Println(user.Name)
    }

## Credits

* [Sam Saffron](http://www.samsaffron.com/) for Dapper.

## License

MIT LICENSE. See [LICENSE](http://olivere.mit-license.org/) or the
LICENSE file in the repository.
