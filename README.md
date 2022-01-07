# Kuery

Simple ORM for .NET

## Install

Install [Kuery](https://www.nuget.org/packages/Kuery) from Nuget.

## Usage

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

```cs
var todo =new Todo()
{
    Name = "Study English",
    Description = "Study English Everyday",
    CreatedAt = DateTimeOffset.Now,
    UpdatedAt = DateTimeOffset.Now,
});

using SqliteConnection connection = new SqliteConnection("Your connection string");
connection.Open();

connection.Insert(todo);

List<Todo> todos = connection.Table<Todo>()
    .Where(x => x.Name == "Study English")
    .ToList();
```

### Asynchronous API

```cs
var todo =new Todo()
{
    Name = "Study English",
    Description = "Study English Everyday",
    CreatedAt = DateTimeOffset.Now,
    UpdatedAt = DateTimeOffset.Now,
});

using SqliteConnection connection = new SqliteConnection("Your connection string");
await connection.OpenAsync();

await connection.InsertAsync(todo);

List<Todo> todos = await connection.Table<Todo>()
    .Where(x => x.Name == "Study English")
    .ToListAsync();
```

#### Manual SQL

```cs
using SqliteConnection connection = new SqliteConnection("Your connection string");
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

