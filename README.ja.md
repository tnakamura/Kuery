# Kuery

.NET 向けのシンプルな ORM

## インストール

NuGet から [Kuery](https://www.nuget.org/packages/Kuery) をインストールしてください。

## 使い方

このライブラリには、テーブルの構成を制御するためのシンプルな属性が含まれています。
例えば、シンプルな TODO プログラムでは次のように使用できます。

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

Kuery の属性に加え、マッパーは以下の .NET 属性にも対応しています。

- `System.ComponentModel.DataAnnotations.Schema.Table`
- `System.ComponentModel.DataAnnotations.Schema.Column`
- `System.ComponentModel.DataAnnotations.Required`
- `System.ComponentModel.DataAnnotations.MaxLength`
- `System.ComponentModel.DataAnnotations.StringLength`

### 同期 API

`Insert` を使用してデータベースに行を挿入できます。
テーブルに自動採番の主キーがある場合は、挿入後にその値を取得できます。

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

`Update` と `Delete` にも同様のメソッドがあります。
すべての書き込みメソッドは、オプションで `IDbTransaction` を受け取ります。

データを検索する最も簡単な方法は、`Table` メソッドを使用することです。
WHERE 句による絞り込みや ORDER BY 句の追加ができます。

```cs
List<Todo> todos = connection.Table<Todo>()
    .Where(x => x.Name == "Study English")
    .ToList();
```

`Table<T>()` は次の操作にも対応しています。

- `OrderBy` / `OrderByDescending` / `ThenBy` / `ThenByDescending`
- `Skip` / `Take` / `ElementAt`
- `Count` / `First` / `FirstOrDefault`
- `Delete()` と `Delete(predicate)`

その他の同期 API：

- 一括書き込み：`InsertAll`、`UpdateAll`、`InsertOrReplace`
- 1 行の取得：`Find(pk)`、`Find(predicate)`、`Get(pk)`、`Get(predicate)`
- SQL の直接実行：`Query<T>(sql, param)`、`Query(type, sql, param)`、`FindWithQuery<T>`、`Execute`、`ExecuteScalar<T>`

### 非同期 API

`InsertAsync` を使用してデータベースに行を挿入できます。
テーブルに自動採番の主キーがある場合は、挿入後にその値を取得できます。

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

`UpdateAsync` と `DeleteAsync` にも同様のメソッドがあります。

データを検索する最も簡単な方法は、`Table` メソッドを使用することです。
このメソッドは `TableQuery` インスタンスを返し、WHERE 句による絞り込みや ORDER BY の追加ができます。
`ToListAsync`、`FirstAsync`、`FirstOrDefaultAsync` のいずれかの取得メソッドが呼び出されるまで、データベースにはアクセスしません。

```cs
List<Todo> todos = await connection.Table<Todo>()
    .Where(x => x.Name == "Study English")
    .ToListAsync();
```

非同期 API は次の操作にも対応しています。

- 一括書き込み：`InsertAllAsync`、`UpdateAllAsync`、`InsertOrReplaceAsync`
- 1 行の取得：`FindAsync(pk)`、`FindAsync(predicate)`、`GetAsync(pk)`、`GetAsync(predicate)`
- LINQ 実行ヘルパー：`ToListAsync`、`CountAsync`、`FirstAsync`、`FirstOrDefaultAsync`
- SQL の直接実行：`QueryAsync<T>(sql, param)`、`FindWithQueryAsync<T>`、`ExecuteAsync`、`ExecuteScalarAsync<T>`

ほとんどの非同期メソッドは `CancellationToken` を受け取ります。

### IQueryable API（`Query<T>()`）

`Query<T>()` を使用して、LINQ で SQL の `SELECT` クエリを組み立てることもできます。

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

現在、SQL への変換では次の操作に対応しています（該当する場合は非同期版を含みます）。

- クエリの構成：`Where`、`Select`、`OrderBy`/`ThenBy`、`Skip`、`Take`
- 取得・集計：`Count`、`LongCount`、`Any`、`All`、`First`、`FirstOrDefault`、`Single`、`SingleOrDefault`、`Last`、`LastOrDefault`、`Sum`、`Min`、`Max`、`Average`
- グループ化：`GroupBy`（単一キーと複合キー）、`HAVING`
- 結合：`Join`、複合キーによる結合、連続した結合、左外部結合（`GroupJoin + SelectMany + DefaultIfEmpty`）
- 集合演算：`Distinct`、`Concat`、`Union`、`Intersect`、`Except`
- サブクエリ：`Contains`（`IN`）、`Any`（`EXISTS`）、`SelectMany`（クロス結合）
- データ変更：`ExecuteDelete`/`ExecuteDeleteAsync`（`Where` が必要）、`ExecuteUpdate`/`ExecuteUpdateAsync`
- 組み込み式の変換：
  - 文字列：`Contains`、`StartsWith`、`EndsWith`、`Replace`、`Substring`、`Trim`、`TrimStart`、`TrimEnd`、`Length`、`IndexOf`、`string.IsNullOrEmpty`、`string.IsNullOrWhiteSpace`
  - 数学：`Math.Abs`、`Math.Round`、`Math.Floor`、`Math.Ceiling`、`Math.Max`、`Math.Min`、`Math.Pow`、`Math.Sqrt`、`Math.Log`、`Math.Log10`
  - 日付・時刻：`DateTime.Now`、`DateTime.UtcNow`、`DateTime.Date`、`DateTime.DayOfWeek`、`AddDays`、`AddMonths`、`AddYears`、`AddHours`、`AddMinutes`、`AddSeconds`、および `Year`、`Month`、`Day`、`Hour`、`Minute`、`Second` などのメンバー
  - 型変換：`Convert.ToInt16`、`Convert.ToInt32`、`Convert.ToInt64`、`Convert.ToDouble`、`Convert.ToSingle`、`Convert.ToBoolean`、`Convert.ToString`、`.ToString()`
  - その他：null 合体演算子（`??`）、三項条件演算子（`?:`）、ビット演算子（`&`、`|`、`^`、`~`）、`KueryFunctions.Like(...)`

サポートされていない LINQ 演算子は `NotSupportedException` をスローします。

#### SQL の直接実行

```cs
using var connection = new Microsoft.Data.Sqlite.SqliteConnection("Your connection string");
connection.Open();

IEnumerable<Todo> todos = connection.Query<Todo>(
    "SELECT * FROM todo WHERE name = $name",
    new { name = "Study English" });
```

非同期の SQL 直接実行 API も使用できます。

```cs
IEnumerable<Todo> todos = await connection.QueryAsync<Todo>(
    "SELECT * FROM todo WHERE name = $name",
    new { name = "Study English" });
```

## テストの実行

テストプロジェクトには 3 つの実行モードがあります。

- `fast`（ローカルでの標準的な実行方法）：SQLite テストのみ
- `integration`：SQL Server、PostgreSQL、MySQL のテスト
- `all`：fast と integration の両方

### 高速テスト（SQLite のみ）

```bash
./test/run-fast-tests.sh
```

### 統合テスト（SQL Server、PostgreSQL、MySQL）

Docker が必要です。

```bash
./test/run-integration-tests.sh
```

このスクリプトは `test/docker-compose.test.yml` を使用してテスト用データベースを起動し、SQL Server、PostgreSQL、MySQL のテストを実行した後、コンテナーを削除します。

### CI

GitHub Actions では次のジョブとワークフローを実行します。

- `fast` ジョブ：SQLite のテストのみ
- `integration` ジョブ：Docker を使用した SQL Server と PostgreSQL のテスト
- `Release NuGet Package` ワークフロー：GitHub Release の公開時に実行され、`NUGET_API_KEY` シークレットを使用してパッケージを NuGet.org にプッシュ

### すべてのテスト

```bash
./test/run-all-tests.sh
```

### 環境変数（任意）

フィクスチャの設定は環境変数で上書きできます。

- PostgreSQL：`KUERY_TEST_PG_HOST`、`KUERY_TEST_PG_PORT`、`KUERY_TEST_PG_USERNAME`、`KUERY_TEST_PG_PASSWORD`、`KUERY_TEST_PG_MASTER_DB`
- SQL Server：`KUERY_TEST_SQLSERVER_HOST`、`KUERY_TEST_SQLSERVER_INTEGRATED_SECURITY`、`KUERY_TEST_SQLSERVER_USERNAME`、`KUERY_TEST_SQLSERVER_PASSWORD`、`KUERY_TEST_SQLSERVER_MASTER_DB`
- MySQL：`KUERY_TEST_MYSQL_HOST`、`KUERY_TEST_MYSQL_PORT`、`KUERY_TEST_MYSQL_USERNAME`、`KUERY_TEST_MYSQL_PASSWORD`、`KUERY_TEST_MYSQL_MASTER_DB`

## 貢献

1. リポジトリをフォークします（http://github.com/tnakamura/Kuery）
2. 機能追加用のブランチを作成します（`git checkout -b my-new-feature`）
3. 変更をコミットします（`git commit -am 'Add some feature'`）
4. ブランチをプッシュします（`git push origin my-new-feature`）
5. 新しいプルリクエストを作成します

## ライセンス

[MIT](https://github.com/tnakamura/Kuery/blob/master/LICENSE.md)

## 作者

[tnakamura](https://github.com/tnakamura)
