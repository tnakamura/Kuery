# Kuery

Simple ORM for .NET

## Install

Install [Kuery](https://www.nuget.org/packages/Kuery) from NuGet.

## Usage

The library contains simple attributes that you can use to control the construction of tables.
In a simple todo program, you might use:

```cs
[Table("todo")]
public class Todo
{
    [PrimaryKey]
    [Column("id")]
    [AutoIncrement]
    public int Id { get; set; }

    [NotNull]
    [Column("name")]
    public string Name { get; set; }

    [Column("description")]
    public string Description { get; set; }

    [NotNull]
    [Column("done")]
    public bool IsDone { get; set; }

    [NotNull]
    [Column("created_at")]
    public DateTimeOffset CreatedAt { get; set; }

    [NotNull]
    [Column("updated_at")]
    public DateTimeOffset UpdatedAt { get; set; }
}
```

### Synchronous API

You can insrt rows in the database using `Insert`.
If the table contains an auto-incremented primary key,
then the value for that key will be available to you after the insert:

```cs
var todo = new Todo()
{
    Name = "Study English",
    Description = "Study English Everyday",
    CreatedAt = DateTimeOffset.Now,
    UpdatedAt = DateTimeOffset.Now,
});

using var connection = new Microsoft.Data.Sqlite.SqliteConnection("Your connection string");
connection.Open();

connection.Insert(todo);
```

Similar methods exist for `Update` and `Delete`.

The most straightforward way to query for data is using the `Table` method.
This can take predicates for constraining via WHERE clauses and/or adding ORDER BY clauses:

```cs
List<Todo> todos = connection.Table<Todo>()
    .Where(x => x.Name == "Study English")
    .ToList();
```


### Asynchronous API

You can insert rows in the database using `InsertAsync`.
If the table contains an auto-incremented primary key,
then the value for that key will be available to after the insert:

```cs
var todo = new Todo()
{
    Name = "Study English",
    Description = "Study English Everyday",
    CreatedAt = DateTimeOffset.Now,
    UpdatedAt = DateTimeOffset.Now,
});

using var connection = new Microsoft.Data.Sqlite.SqliteConnection("Your connection string");
await connection.OpenAsync();

await connection.InsertAsync(todo);
```

Simillar methods exist for `UpdateAsync` and `DeleteAsync`.

Querying for data is most straightforwardly done using the `Table` method.
This will return an `TableQuery` instance back,
whererupon you can add predicates for constraining via WHERE clauses and/or adding ORDER BY.
The database is not physically touched until one of the special retrieval
methods - `ToListAsync`, `FirstAsync`, or `FirstOrDefaultAsync` - is called.

```cs
List<Todo> todos = await connection.Table<Todo>()
    .Where(x => x.Name == "Study English")
    .ToListAsync();
```

### IQueryable API (`Query<T>()`)

You can also compose SQL `SELECT` queries through LINQ by using `Query<T>()`.

```cs
var products = await connection.Query<Product>()
    .Where(p => p.Code == "0001")
    .OrderBy(p => p.Code)
    .ToListAsync();
```

In the current implementation, SQL translation supports:

- `Where`
- `OrderBy` / `ThenBy`
- `Skip` / `Take`
- `Count`
- `First` / `FirstOrDefault`

Unsupported LINQ operators throw `NotSupportedException`.

#### Manual SQL

```cs
using var connection = new Microsoft.Data.Sqlite.SqliteConnection("Your connection string");
connection.Open();

IEnumerable<Todo> todos = connection.Query<Todo>(
    "SELECT * FROM todo WHERE name = $name",
    new { name = "Study English" });
```

## Contribution

1. Fork it ( http://github.com/tnakamura/Kuery )
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create new Pull Request

## License

[MIT](https://github.com/tnakamura/Kuery/blob/master/LICENSE.md)

## Author

[tnakamura](https://github.com/tnakamura)

