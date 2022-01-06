# Kuery

Simple ORM for .NET

## Install

Install [Kuery](https://www.nuget.org/packages/Kuery) from Nuget.

## Usage

### Synchronous API

```cs
using SqliteConnection connection = new SqliteConnection("Your connection string");
connection.Open();

List<Company> companies = connection.Table<Company>().Where(x => x.Name == "Google").ToList();
```

### Asynchronous API

```cs
using SqliteConnection connection = new SqliteConnection("Your connection string");
await connection.OpenAsync();

List<Company> companies = await connection.Table<Company>().Where(x => x.Name == "Google").ToListAsync();
```

#### Manual SQL

```cs
using SqliteConnection connection = new SqliteConnection("Your connection string");
connection.Open();

IEnumerable<Company> companies = connection.Query<Company>(
    "SELECT * FROM companies WHERE name = $Name",
    new { Name = "Google" });
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

