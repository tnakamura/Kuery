# IQueryable SQL サポート状況

`connection.Query<T>()` が返す `IQueryable<T>` を通じて発行できる SQL の対応状況をまとめる。

---

## 対応済み機能

### LINQ 演算子

| カテゴリ | LINQ メソッド | 生成される SQL | 備考 |
|----------|-------------|--------------|------|
| フィルタ | `Where(predicate)` | `WHERE ...` | 複数チェインは `AND` で結合 |
| 射影 | `Select(x => x.Prop)` | `SELECT col` | 単一カラム |
| 射影 | `Select(x => new { ... })` | `SELECT col1, col2, ...` | 匿名型 |
| 射影 | `Select(x => new T { ... })` | `SELECT col1, col2, ...` | MemberInit |
| ソート | `OrderBy(x => x.Prop)` | `ORDER BY col ASC` | |
| ソート | `OrderByDescending(x => x.Prop)` | `ORDER BY col DESC` | |
| ソート | `ThenBy(x => x.Prop)` | `, col ASC` | 複合ソート |
| ソート | `ThenByDescending(x => x.Prop)` | `, col DESC` | 複合ソート |
| ページング | `Skip(n)` | `OFFSET n` | 方言により構文が変わる |
| ページング | `Take(n)` | `LIMIT n` / `TOP n` / `FETCH NEXT n` | 方言により構文が変わる |
| 集計 | `Count()` / `Count(predicate)` | `SELECT count(*)` | |
| 集計 | `LongCount()` / `LongCount(predicate)` | `SELECT count(*)` | |
| 集計 | `Sum(selector)` | `SELECT sum(col)` | |
| 集計 | `Min(selector)` | `SELECT min(col)` | |
| 集計 | `Max(selector)` | `SELECT max(col)` | |
| 集計 | `Average(selector)` | `SELECT avg(col)` | |
| 要素取得 | `First()` / `First(predicate)` | `LIMIT 1` / `TOP 1` | |
| 要素取得 | `FirstOrDefault()` / `FirstOrDefault(predicate)` | `LIMIT 1` / `TOP 1` | 結果なし時 `default` |
| 要素取得 | `Last()` / `Last(predicate)` | `ORDER BY ... DESC LIMIT 1` | ソート方向を反転して先頭取得 |
| 要素取得 | `LastOrDefault()` / `LastOrDefault(predicate)` | `ORDER BY ... DESC LIMIT 1` | |
| 要素取得 | `Single()` / `Single(predicate)` | `LIMIT 2` / `TOP 2` | 2 件取得し件数検証 |
| 要素取得 | `SingleOrDefault()` / `SingleOrDefault(predicate)` | `LIMIT 2` / `TOP 2` | |
| 要素取得 | `ElementAt(index)` | `OFFSET n LIMIT 1` | |
| 要素取得 | `ElementAtOrDefault(index)` | `OFFSET n LIMIT 1` | |
| 存在チェック | `Any()` / `Any(predicate)` | `SELECT count(*)` → `> 0` 判定 | |
| 全件チェック | `All(predicate)` | `SELECT count(*) WHERE NOT(predicate)` → `== 0` 判定 | |
| 重複排除 | `Distinct()` | `SELECT DISTINCT ...` | |
| 結合 | `Join(inner, outerKey, innerKey, resultSelector)` | `INNER JOIN ... ON ...` | 単一キーのみ |
| 結合 | `Join(inner, x => new { x.A, x.B }, y => new { y.A, y.B }, ...)` | `INNER JOIN ... ON t1.A = t2.A AND t1.B = t2.B` | 複合キー |
| 結合 | 複数 `Join` チェイン | `INNER JOIN t2 ... INNER JOIN t3 ...` | 3 テーブル以上 |
| 結合 | `GroupJoin` + `SelectMany` + `DefaultIfEmpty` | `LEFT JOIN ... ON ...` | |
| グループ化 | `GroupBy(x => x.Prop)` | `GROUP BY col` | 単一キー |
| グループ化 | `GroupBy(x => new { x.A, x.B })` | `GROUP BY colA, colB` | 複合キー |
| グループ化 | `.GroupBy(...).Select(g => new { g.Key, Count = g.Count() })` | `SELECT col, count(*)` | 集計関数と組合せ |
| HAVING | `.GroupBy(...).Where(g => g.Count() > n).Select(...)` | `HAVING count(*) > n` | GroupBy の後の Where が HAVING に変換される |
| サブクエリ | `subQuery.Contains(x.Prop)` | `col IN (SELECT ...)` | IQueryable を使ったサブクエリ |
| サブクエリ | `query.Any(predicate)` in Where | `EXISTS (SELECT ...)` | 相関サブクエリ対応 |
| クロス結合 | `SelectMany(x => query)` | `CROSS JOIN` | 結果セレクタ有無両対応 |
| 集合演算 | `Union()` | `UNION` | 重複を除いた和集合 |
| 集合演算 | `Concat()` | `UNION ALL` | 重複を含む和集合 |
| 集合演算 | `Intersect()` | `INTERSECT` | 積集合 |
| 集合演算 | `Except()` | `EXCEPT` | 差集合 |

### WHERE 述語で使える式

| カテゴリ | C# 式 | 生成される SQL | 備考 |
|----------|-------|--------------|------|
| 比較 | `==`, `!=`, `>`, `>=`, `<`, `<=` | `=`, `!=`, `>`, `>=`, `<`, `<=` | |
| 論理 | `&&`, `\|\|`, `!` | `AND`, `OR`, `NOT` | |
| 算術 | `+`, `-`, `*`, `/`, `%` | `+`, `-`, `*`, `/`, `%` | |
| null チェック | `x.Prop == null` | `col IS NULL` | |
| null チェック | `x.Prop != null` | `col IS NOT NULL` | |
| Nullable | `x.Prop.HasValue` | `col IS NOT NULL` | |
| Nullable | `!x.Prop.HasValue` | `col IS NULL` | |
| IN 句 | `list.Contains(x.Prop)` | `col IN (...)` | `Enumerable.Contains` も対応 |
| Equals | `x.Prop.Equals(value)` | `col = @p` | |
| 条件式 | `condition ? a : b` | `CASE WHEN ... THEN ... ELSE ... END` | |
| 文字列 | `x.Name.Contains("abc")` | `instr(col, @p) > 0` | SqlServer: `CHARINDEX`, PostgreSQL: `strpos` |
| 文字列 | `x.Name.StartsWith("abc")` | `substr(col, 1, n) = @p` | `StringComparison` 指定時は `LIKE` |
| 文字列 | `x.Name.EndsWith("abc")` | `substr(col, length(col) - n + 1, n) = @p` | `StringComparison` 指定時は `LIKE` |
| 文字列 | `x.Name.Replace("a", "b")` | `replace(col, @p1, @p2)` | |
| 文字列 | `x.Name.ToLower()` | `lower(col)` | |
| 文字列 | `x.Name.ToUpper()` | `upper(col)` | |
| 文字列 | `x.Name.Trim()` | `trim(col)` | |
| 文字列 | `x.Name.TrimStart()` | `ltrim(col)` | |
| 文字列 | `x.Name.TrimEnd()` | `rtrim(col)` | |
| 文字列 | `x.Name.Substring(start)` | `substr(col, start+1)` | SqlServer: `SUBSTRING` |
| 文字列 | `x.Name.Substring(start, len)` | `substr(col, start+1, len)` | SqlServer: `SUBSTRING` |
| 文字列 | `x.Name.IndexOf("a")` | `instr(col, @p) - 1` | SqlServer: `CHARINDEX`, PostgreSQL: `strpos` |
| 文字列 | `x.Name.Length` | `length(col)` | SqlServer: `LEN` |
| 文字列 | `string.IsNullOrEmpty(x.Name)` | `col IS NULL OR col = ''` | |
| 文字列 | `string.IsNullOrWhiteSpace(x.Name)` | `col IS NULL OR TRIM(col) = ''` | |
| 文字列結合 | `x.A + x.B` (string 型) | `col1 \|\| col2` | SqlServer: `col1 + col2` |
| 文字列結合 | `string.Concat(x.A, x.B)` | `col1 \|\| col2` | SqlServer: `col1 + col2` |
| 数学 | `Math.Abs(x.Value)` | `abs(col)` | |
| 数学 | `Math.Round(x.Value)` | `round(col)` | |
| 数学 | `Math.Round(x.Value, digits)` | `round(col, digits)` | |
| 数学 | `Math.Floor(x.Value)` | `FLOOR(col)` / `floor(col)` | SQLite: CASE 式で代替 |
| 数学 | `Math.Ceiling(x.Value)` | `CEILING(col)` / `ceil(col)` | SQLite: CASE 式で代替 |
| 数学 | `Math.Max(x.A, x.B)` | `max(a, b)` | |
| 数学 | `Math.Min(x.A, x.B)` | `min(a, b)` | |
| 数学 | `Math.Pow(x, y)` | `POWER(x, y)` / `power(x, y)` | |
| 数学 | `Math.Sqrt(x)` | `SQRT(x)` / `sqrt(x)` | |
| 数学 | `Math.Log(x)` | `LOG(x)` | PostgreSQL/SQLite: `ln(x)` |
| 数学 | `Math.Log(x, newBase)` | `LOG(x, newBase)` | PostgreSQL/SQLite: `ln(x) / ln(newBase)` |
| 数学 | `Math.Log10(x)` | `LOG10(x)` / `log10(x)` | |
| null 合体 | `x.Prop ?? defaultValue` | `COALESCE(col, default)` | |
| 日時 | `x.Date.Year` | `DATEPART(year, col)` | PostgreSQL: `EXTRACT`, SQLite: `strftime('%Y')` |
| 日時 | `x.Date.Month` | `DATEPART(month, col)` | PostgreSQL: `EXTRACT`, SQLite: `strftime('%m')` |
| 日時 | `x.Date.Day` | `DATEPART(day, col)` | PostgreSQL: `EXTRACT`, SQLite: `strftime('%d')` |
| 日時 | `x.Date.Hour` | `DATEPART(hour, col)` | PostgreSQL: `EXTRACT`, SQLite: `strftime('%H')` |
| 日時 | `x.Date.Minute` | `DATEPART(minute, col)` | PostgreSQL: `EXTRACT`, SQLite: `strftime('%M')` |
| 日時 | `x.Date.Second` | `DATEPART(second, col)` | PostgreSQL: `EXTRACT`, SQLite: `strftime('%S')` |
| 日時 | `x.Date.Date` | `CAST(CAST(col AS date) AS datetime)` | PostgreSQL: `date_trunc('day', col)`, SQLite: `strftime('%Y-%m-%d 00:00:00', col)` |
| 日時 | `x.Date.DayOfWeek` | `DATEPART(weekday, col) - 1` | PostgreSQL: `EXTRACT(dow)`, SQLite: `strftime('%w', col)` |
| 日時 | `DateTime.Now` | `GETDATE()` | PostgreSQL: `LOCALTIMESTAMP`, SQLite: `datetime('now', 'localtime')` |
| 日時 | `DateTime.UtcNow` | `GETUTCDATE()` | PostgreSQL: `NOW() AT TIME ZONE 'UTC'`, SQLite: `datetime('now')` |
| 日時演算 | `x.Date.AddDays(n)` | `DATEADD(day, n, col)` | PostgreSQL: `col + make_interval(...)`, SQLite: `datetime(col, 'n days')` |
| 日時演算 | `x.Date.AddMonths(n)` | `DATEADD(month, n, col)` | PostgreSQL: `col + make_interval(...)`, SQLite: `datetime(col, 'n months')` |
| 日時演算 | `x.Date.AddYears(n)` | `DATEADD(year, n, col)` | PostgreSQL: `col + make_interval(...)`, SQLite: `datetime(col, 'n years')` |
| 日時演算 | `x.Date.AddHours(n)` | `DATEADD(hour, n, col)` | PostgreSQL: `col + make_interval(...)`, SQLite: `datetime(col, 'n hours')` |
| 日時演算 | `x.Date.AddMinutes(n)` | `DATEADD(minute, n, col)` | PostgreSQL: `col + make_interval(...)`, SQLite: `datetime(col, 'n minutes')` |
| 日時演算 | `x.Date.AddSeconds(n)` | `DATEADD(second, n, col)` | PostgreSQL: `col + make_interval(...)`, SQLite: `datetime(col, 'n seconds')` |
| 型変換 | `Convert.ToInt16(x.Prop)` | `CAST(col AS int)` | SQLite/PostgreSQL: `integer` |
| 型変換 | `Convert.ToInt32(x.Prop)` | `CAST(col AS int)` | SQLite/PostgreSQL: `integer` |
| 型変換 | `Convert.ToInt64(x.Prop)` | `CAST(col AS bigint)` | SQLite/PostgreSQL: `integer` |
| 型変換 | `Convert.ToDouble(x.Prop)` | `CAST(col AS float)` | SQLite/PostgreSQL: `real` |
| 型変換 | `Convert.ToSingle(x.Prop)` | `CAST(col AS real)` | |
| 型変換 | `Convert.ToBoolean(x.Prop)` | `CAST(col AS bit)` | SQLite/PostgreSQL: `integer` |
| 型変換 | `Convert.ToString(x.Prop)` | `CAST(col AS nvarchar(max))` | SQLite/PostgreSQL: `text` |
| 型変換 | `x.Prop.ToString()` | `CAST(col AS nvarchar(max))` | SQLite/PostgreSQL: `text` |
| LIKE | `KueryFunctions.Like(x.Name, "%pattern%")` | `col LIKE @p` | SQL LIKE ワイルドカード (`%`, `_`) を使用 |
| ビット演算 | `x.Flags & mask` | `col & mask` | |
| ビット演算 | `x.Flags \| mask` | `col \| mask` | |
| ビット演算 | `x.Flags ^ mask` | `col ^ mask` | SQLite: `(col \| mask) - (col & mask)` で代替 |
| ビット演算 | `~x.Flags` | `~col` | |

### 対応 SQL 方言

| 方言 | ページング構文 | 識別子クオート | パラメータ書式 |
|------|-------------|-------------|-------------|
| SQLite | `LIMIT n OFFSET m` | `` `name` `` | `@p1` |
| SQL Server | `TOP n` / `OFFSET m ROWS FETCH NEXT n ROWS ONLY` | `[name]` | `@p1` |
| PostgreSQL | `LIMIT n OFFSET m` | `"name"` | `@p1` |

---

## 未対応機能

### LINQ 演算子

| カテゴリ | LINQ メソッド / パターン | 対応する SQL | 優先度 |
|----------|----------------------|------------|--------|
| その他 | `Reverse()` | ソート反転 | 低 |
| その他 | `SkipWhile(predicate)` | N/A（SQL で直接表現不可） | 対応予定なし |
| その他 | `TakeWhile(predicate)` | N/A（SQL で直接表現不可） | 対応予定なし |
| その他 | `Zip()` | N/A（SQL で直接表現不可） | 対応予定なし |
| その他 | `Aggregate()` | N/A（汎用集約は SQL で表現困難） | 対応予定なし |
| その他 | `DefaultIfEmpty()` | N/A（LEFT JOIN パターンの一部） | 低 |
| その他 | `OfType<T>()` / `Cast<T>()` | N/A | 対応予定なし |
| その他 | `SequenceEqual()` | N/A | 対応予定なし |
| その他 | `Append()` / `Prepend()` | N/A | 対応予定なし |

### WHERE 述語の式

| カテゴリ | C# 式 | 対応する SQL | 優先度 |
|----------|-------|------------|--------|
| 文字列 | `string.Format(...)` | 文字列結合に展開 | 低 |
| 文字列 | `string.Join(sep, ...)` | N/A（集約コンテキスト依存） | 低 |
| 文字列 | `string.Concat(a, b, c)` (3 引数以上) | 文字列結合に展開 | 低 |
| 文字列 | `string.PadLeft(n)` / `string.PadRight(n)` | `LPAD` / `RPAD` | 低 |
| 数学 | `Math.Truncate(x)` | `TRUNC` / `TRUNCATE` | 低 |
| 数学 | `Math.Sign(x)` | `SIGN(x)` | 低 |
| 数学 | `Math.Exp(x)` | `EXP(x)` | 低 |
| 日時 | `TimeSpan` 演算 | 日時差分（`DATEDIFF` 等） | 低 |
| 日時 | `DateTimeOffset` 操作 | タイムゾーン付き日時 | 低 |

### SQL 固有機能

| 機能 | SQL 構文 | 優先度 |
|------|---------|--------|
| ウィンドウ関数 | `ROW_NUMBER() OVER(...)`, `RANK()`, etc. | 低 |
| CTE | `WITH cte AS (...)` | 低 |
| `RIGHT JOIN` | `RIGHT JOIN ... ON ...` | 低 |
| `FULL OUTER JOIN` | `FULL OUTER JOIN ... ON ...` | 低 |
| `DISTINCT` + 集計関数 | `COUNT(DISTINCT col)`, `SUM(DISTINCT col)` 等 | 中 |
| SELECT 内の計算式 | `SELECT col1 + col2`, `SELECT UPPER(col)` 等 | 中 |
| ORDER BY 内の式 | `ORDER BY length(col)`, `ORDER BY col1 + col2` 等 | 低 |
| GROUP BY 内の式 | `GROUP BY YEAR(col)` 等 | 低 |

---

## 実装計画

優先度が高い順に実装する。各フェーズは独立してリリース可能。

### Phase 1: 結合の強化（✅ 実装済み）

JOIN は実用上の利用頻度が非常に高い。現状 INNER JOIN の単一キーのみ対応しているため、まず結合機能を拡充する。

| # | 機能 | 概要 |
|---|------|------|
| 1-1 | ✅ LEFT JOIN | `GroupJoin` + `SelectMany` + `DefaultIfEmpty` パターンを認識して `LEFT JOIN` を生成 |
| 1-2 | ✅ 複合キー JOIN | `Join(inner, x => new { x.A, x.B }, y => new { y.A, y.B }, ...)` で複数 ON 条件を生成 |
| 1-3 | ✅ 複数テーブル JOIN | `Join` を複数回チェインした場合に 3 テーブル以上の結合を可能にする |

### Phase 2: 式の拡張（✅ 実装済み）

WHERE 述語で使える式を増やし、より複雑な条件を書けるようにする。

| # | 機能 | 概要 |
|---|------|------|
| 2-1 | ✅ null 合体演算子 (`??`) | `COALESCE` への変換 |
| 2-2 | ✅ `string.IsNullOrWhiteSpace()` | `IS NULL OR TRIM(col) = ''` への変換 |
| 2-3 | ✅ `DateTime.Now` / `DateTime.UtcNow` | 各方言の現在時刻関数への変換 |
| 2-4 | ✅ `DateTime.AddDays()` 等の日時演算 | `DATEADD` / SQLite の `datetime()` への変換 |

### Phase 3: 型変換（✅ 実装済み）

| # | 機能 | 概要 |
|---|------|------|
| 3-1 | ✅ `Convert.ToInt32()` 等 | `CAST(col AS int)` への変換 |
| 3-2 | ✅ `ToString()` | `CAST(col AS text)` への変換 |
| 3-3 | ✅ カスタム LIKE パターン | `KueryFunctions.Like()` ヘルパーメソッドによる `LIKE` パターン検索 |

### Phase 4: 集合演算（✅ 実装済み）

| # | 機能 | 概要 |
|---|------|------|
| 4-1 | ✅ `Union()` | `UNION` への変換 |
| 4-2 | ✅ `Concat()` | `UNION ALL` への変換 |
| 4-3 | ✅ `Intersect()` | `INTERSECT` への変換 |
| 4-4 | ✅ `Except()` | `EXCEPT` への変換 |

### Phase 5: サブクエリ（✅ 実装済み）

| # | 機能 | 概要 |
|---|------|------|
| 5-1 | ✅ WHERE 内サブクエリ | `col IN (SELECT ...)` パターン |
| 5-2 | ✅ EXISTS サブクエリ | `WHERE EXISTS (SELECT ...)` パターン |
| 5-3 | ✅ `SelectMany()` | CROSS JOIN パターン |

### Phase 6: 追加の数学・日時関数（✅ 実装済み）

| # | 機能 | 概要 |
|---|------|------|
| 6-1 | ✅ `Math.Pow()` / `Math.Sqrt()` | `POWER` / `SQRT` 関数 |
| 6-2 | ✅ `Math.Log()` / `Math.Log10()` | `LOG` / `LOG10` 関数 |
| 6-3 | ✅ `DateTime.Date` / `DateTime.DayOfWeek` | 日付部分の取得 |
| 6-4 | ✅ ビット演算 | `&`, `\|`, `^`, `~` の SQL 変換 |

### 対応予定なし

以下は SQL で直接表現が困難、または利用頻度が極めて低いため対応しない。必要な場合は生 SQL (`connection.Query<T>(sql)`) を使用する。

- `SkipWhile()` / `TakeWhile()` — SQL に直接対応する構文がない
- `Zip()` — SQL に直接対応する構文がない
- `Aggregate()` — 汎用集約は SQL で表現困難
- `OfType<T>()` / `Cast<T>()` — 単一テーブルマッピングのため不要
- `SequenceEqual()` — SQL に直接対応する構文がない
- `Append()` / `Prepend()` — SQL に直接対応する構文がない
- ウィンドウ関数 / CTE — LINQ 式木では直接表現が困難。生 SQL を推奨

---

## 制約事項

- `Select` 内では直接のプロパティアクセスのみ対応（計算式やメソッド呼び出しは不可）。
- `OrderBy` 内では直接のプロパティアクセスのみ対応（`OrderBy(x => x.Name.Length)` のような式は不可）。
- `GroupBy` 内では直接のプロパティアクセスのみ対応（`GroupBy(x => x.Date.Year)` のような式は不可）。
- `Join` は単一キー / 複合キーの INNER JOIN、および `GroupJoin` + `SelectMany` + `DefaultIfEmpty` による LEFT JOIN に対応。複数テーブルへの JOIN チェインも可能。`RIGHT JOIN` / `FULL OUTER JOIN` は未対応。
- `SelectMany` はルートクエリ（`connection.Query<T>()`）を返すコレクションセレクタのみ対応（CROSS JOIN を生成）。
- サブクエリ（`Contains` → `IN (SELECT ...)`、`Any` → `EXISTS (SELECT ...)`）は `IQueryable` をソースとする場合のみ対応。
- `Distinct()` と集計関数の組み合わせ（`query.Select(x => x.Name).Distinct().Count()` → `COUNT(DISTINCT col)` 等）は未対応。`Distinct()` は通常の `SELECT DISTINCT` としてのみ機能する。
- `string.Concat()` は 2 引数のみ対応。3 引数以上の `string.Concat(a, b, c)` は未対応（ただし `x.A + x.B + x.C` のような `+` 演算子チェインは対応済み）。
- 未対応の演算子を使用した場合は `NotSupportedException` がスローされる（クライアント評価へのフォールバックは行わない）。
