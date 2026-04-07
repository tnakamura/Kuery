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

In addition to Kuery attributes, the mapper also supports these .NET attributes:

- `System.ComponentModel.DataAnnotations.Schema.Table`
- `System.ComponentModel.DataAnnotations.Schema.Column`
- `System.ComponentModel.DataAnnotations.Required`
- `System.ComponentModel.DataAnnotations.MaxLength`
- `System.ComponentModel.DataAnnotations.StringLength`

### Synchronous API

You can insert rows in the database using `Insert`.
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
All write methods accept an optional `IDbTransaction`.

The most straightforward way to query for data is using the `Table` method.
This can take predicates for constraining via WHERE clauses and/or adding ORDER BY clauses:

```cs
List<Todo> todos = connection.Table<Todo>()
    .Where(x => x.Name == "Study English")
    .ToList();
```

`Table<T>()` also supports:

- `OrderBy` / `OrderByDescending` / `ThenBy` / `ThenByDescending`
- `Skip` / `Take` / `ElementAt`
- `Count` / `First` / `FirstOrDefault`
- `Delete()` and `Delete(predicate)`

Other synchronous APIs:

- Batch writes: `InsertAll`, `UpdateAll`, `InsertOrReplace`
- Single row retrieval: `Find(pk)`, `Find(predicate)`, `Get(pk)`, `Get(predicate)`
- Manual SQL: `Query<T>(sql, param)`, `Query(type, sql, param)`, `FindWithQuery<T>`, `Execute`, `ExecuteScalar<T>`


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

Similar methods exist for `UpdateAsync` and `DeleteAsync`.

Querying for data is most straightforwardly done using the `Table` method.
This will return a `TableQuery` instance back,
whereupon you can add predicates for constraining via WHERE clauses and/or adding ORDER BY.
The database is not physically touched until one of the special retrieval
methods - `ToListAsync`, `FirstAsync`, or `FirstOrDefaultAsync` - is called.

```cs
List<Todo> todos = await connection.Table<Todo>()
    .Where(x => x.Name == "Study English")
    .ToListAsync();
```

Async API also provides:

- Batch writes: `InsertAllAsync`, `UpdateAllAsync`, `InsertOrReplaceAsync`
- Single row retrieval: `FindAsync(pk)`, `FindAsync(predicate)`, `GetAsync(pk)`, `GetAsync(predicate)`
- LINQ execution helpers: `ToListAsync`, `CountAsync`, `FirstAsync`, `FirstOrDefaultAsync`
- Manual SQL: `QueryAsync<T>(sql, param)`, `FindWithQueryAsync<T>`, `ExecuteAsync`, `ExecuteScalarAsync<T>`

Most async methods accept `CancellationToken`.

### IQueryable API (`Query<T>()`)

You can also compose SQL `SELECT` queries through LINQ by using `Query<T>()`.

```cs
var products = await connection.Query<Product>()
    .Where(p => p.Code == "0001")
    .OrderBy(p => p.Code)
    .ToListAsync();

var deleted = await connection.Query<Product>()
    .Where(p => p.Id == id)
    .ExecuteDeleteAsync();

var updated = await connection.Query<Product>()
    .Where(p => p.Code == "0001")
    .ExecuteUpdateAsync(setters => setters
        .SetProperty(p => p.Name, p => p.Name + " (updated)"));
```

SQL translation currently supports (including async equivalents where applicable):

- Query shape: `Where`, `Select`, `OrderBy`/`ThenBy`, `Skip`, `Take`
- Retrieval/aggregation: `Count`, `LongCount`, `Any`, `All`, `First`, `FirstOrDefault`, `Single`, `SingleOrDefault`, `Last`, `LastOrDefault`, `Sum`, `Min`, `Max`, `Average`
- Grouping: `GroupBy` (single key and composite key), `HAVING`
- Joins: `Join`, composite-key join, chained joins, left join (`GroupJoin + SelectMany + DefaultIfEmpty`)
- Set operations: `Distinct`, `Concat`, `Union`, `Intersect`, `Except`
- Subqueries: `Contains` (`IN`), `Any` (`EXISTS`), `SelectMany` (cross join)
- Data modification: `ExecuteDelete`/`ExecuteDeleteAsync` (requires `Where`), `ExecuteUpdate`/`ExecuteUpdateAsync`
- Built-in expression translation:
  - String: `Contains`, `StartsWith`, `EndsWith`, `Replace`, `Substring`, `Trim`, `TrimStart`, `TrimEnd`, `Length`, `IndexOf`, `string.IsNullOrEmpty`, `string.IsNullOrWhiteSpace`
  - Math: `Math.Abs`, `Math.Round`, `Math.Floor`, `Math.Ceiling`, `Math.Max`, `Math.Min`, `Math.Pow`, `Math.Sqrt`, `Math.Log`, `Math.Log10`
  - Date/Time: `DateTime.Now`, `DateTime.UtcNow`, `DateTime.Date`, `DateTime.DayOfWeek`, `AddDays`, `AddMonths`, `AddYears`, `AddHours`, `AddMinutes`, `AddSeconds`, and members such as `Year`, `Month`, `Day`, `Hour`, `Minute`, `Second`
  - Type conversion: `Convert.ToInt16`, `Convert.ToInt32`, `Convert.ToInt64`, `Convert.ToDouble`, `Convert.ToSingle`, `Convert.ToBoolean`, `Convert.ToString`, `.ToString()`
  - Other: null coalescing (`??`), ternary conditional (`?:`), bitwise operators (`&`, `|`, `^`, `~`), and `KueryFunctions.Like(...)`

Unsupported LINQ operators throw `NotSupportedException`.

#### Manual SQL

```cs
using var connection = new Microsoft.Data.Sqlite.SqliteConnection("Your connection string");
connection.Open();

IEnumerable<Todo> todos = connection.Query<Todo>(
    "SELECT * FROM todo WHERE name = $name",
    new { name = "Study English" });
```

You can also use async manual SQL APIs:

```cs
IEnumerable<Todo> todos = await connection.QueryAsync<Todo>(
    "SELECT * FROM todo WHERE name = $name",
    new { name = "Study English" });
```

## Running Tests

The test project has three execution modes:

- `fast` (default local workflow): SQLite tests only
- `integration`: SQL Server + PostgreSQL + MySQL tests
- `all`: fast + integration

### Fast (SQLite only)

```bash
./test/run-fast-tests.sh
```

### Integration (SQL Server + PostgreSQL + MySQL)

Requires Docker.

```bash
./test/run-integration-tests.sh
```

This script starts test databases via `test/docker-compose.test.yml`, runs SQL Server/PostgreSQL/MySQL tests, then cleans up containers.

### CI

GitHub Actions runs:

- `fast` job: SQLite-only tests
- `integration` job: SQL Server + PostgreSQL tests via Docker
- `Release NuGet Package` workflow: runs when a GitHub Release is published and pushes package(s) to NuGet.org using `NUGET_API_KEY` secret

### All tests

```bash
./test/run-all-tests.sh
```

### Environment variables (optional)

Fixtures now support environment variable overrides.

- PostgreSQL: `KUERY_TEST_PG_HOST`, `KUERY_TEST_PG_PORT`, `KUERY_TEST_PG_USERNAME`, `KUERY_TEST_PG_PASSWORD`, `KUERY_TEST_PG_MASTER_DB`
- SQL Server: `KUERY_TEST_SQLSERVER_HOST`, `KUERY_TEST_SQLSERVER_INTEGRATED_SECURITY`, `KUERY_TEST_SQLSERVER_USERNAME`, `KUERY_TEST_SQLSERVER_PASSWORD`, `KUERY_TEST_SQLSERVER_MASTER_DB`
- MySQL: `KUERY_TEST_MYSQL_HOST`, `KUERY_TEST_MYSQL_PORT`, `KUERY_TEST_MYSQL_USERNAME`, `KUERY_TEST_MYSQL_PASSWORD`, `KUERY_TEST_MYSQL_MASTER_DB`

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
