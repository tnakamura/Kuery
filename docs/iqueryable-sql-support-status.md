# IQueryable SQL サポート状況

`connection.Query<T>()` が返す `IQueryable<T>` を対象とする。
ライブラリは .NET Standard 2.0、開発・テストは .NET 10。
EF Core への依存、変更追跡、マイグレーションは含まない。

## LINQ 演算子

| 分類 | 対応範囲 |
|---|---|
| 絞り込み | Where。複数条件、射影・ページング後の絞り込み |
| 射影 | スカラー、匿名型、コンストラクター、MemberInit、エンティティ。計算式・対応関数も使用可能 |
| ソート | OrderBy / OrderByDescending / ThenBy / ThenByDescending。計算式と射影先メンバーも使用可能 |
| ページング | Skip(int) / Take(int)。繰り返しと演算順序を保持。負の Skip は0、負の Take は空集合 |
| 重複排除 | Distinct。射影後の Distinct、Distinct 後の Count / 集約 |
| 集約 | Count / LongCount / Sum / Min / Max / Average。述語・セレクター付きの標準オーバーロード |
| 存在・全件判定 | Any / All / 最上位の Contains。null 補償付き EXISTS / NOT EXISTS。エンティティ Contains は既存の単一主キーで比較 |
| 要素取得 | First / FirstOrDefault / Single / SingleOrDefault / Last / LastOrDefault / ElementAt(int) / ElementAtOrDefault(int) |
| 結合 | Join。単一・複合キー、複数結合、自己結合、左右の入力クエリの絞り込み・射影・ページング |
| 外部結合 | LeftJoin / RightJoin / FullJoin、従来の標準 LEFT JOIN パターン。独立した左右の入力と、結合後の合成 |
| 逆順・型・既定値 | Reverse（明示的な並び順が必要）、Cast、OfType（派生型は TPH）、引数なし DefaultIfEmpty |
| クロス結合 | 外側に依存しない SelectMany。結果セレクターの有無をサポート |
| グループ化 | GroupBy の単一・複合・計算式キー、キーと集約の Select、HAVING。集約結果への追加の絞り込み・ソート |
| 集合演算 | Union / Concat / Intersect / Except。入れ子、各入力のページング、演算後の Where / Select / ソート / ページング / 集約 |
| サブクエリ | IQueryable の Contains / Any / All / 集約。相関条件と SELECT 内のスカラー集約 |

例:

```cs
var query = connection.Query<Product>()
    .Select(p => new { p.Name, Score = p.Price * p.TotalSales })
    .Where(p => p.Score > 100)
    .OrderByDescending(p => p.Score);

var result = await query.Take(10).ToListAsync();
var count = await query.CountAsync();
```

## SQL に変換できる式

同じ式変換を SELECT / WHERE / ORDER BY / GROUP BY / HAVING / 更新値で共有する。

| 分類 | 対応範囲 |
|---|---|
| 基本演算 | 比較、論理、四則演算、剰余、条件式、null 合体、ビット演算 |
| null | == / !=、HasValue、Value、GetValueOrDefault、null を含むコレクションの Contains |
| 文字列 | Contains / StartsWith / EndsWith、Replace、ToLower / ToUpper、Trim / TrimStart / TrimEnd、Substring、IndexOf(string)、Length |
| 文字列の補助 | IsNullOrEmpty / IsNullOrWhiteSpace、+ / Concat(string, ...)、Equals、KueryFunctions.Like |
| 数学 | Abs / Round / Floor / Ceiling / Max / Min / Pow / Sqrt / Log / Log10 |
| 日時 | DateTime.Now / UtcNow / Today、Year / Month / Day / Hour / Minute / Second / Date / DayOfWeek、AddDays / AddMonths / AddYears / AddHours / AddMinutes / AddSeconds |
| 型変換 | 数値・bool・string への対応する Convert メソッド、数値・string の引数なし ToString、基本的なキャスト |

StartsWith / EndsWith / Contains の StringComparison 指定は、無視する大小文字の指定を
SQL の lower に変換する。文字列の照合・Unicode・カルチャ、日時や数値の精度と範囲は
DB の規則に依存し、すべての .NET のオーバーロードや境界条件を再現するものではない。

## DB と実行 API

| DB | ページング | 識別子 | パラメータ |
|---|---|---|---|
| SQLite | LIMIT / OFFSET | `"name"` | `$p1` |
| PostgreSQL | LIMIT / OFFSET | `"name"` | `@p1` |
| SQL Server | TOP / OFFSET FETCH | `[name]` | `@p1` |
| MySQL | LIMIT / OFFSET | `` `name` `` | `@p1` |

- 非同期取得は既存の ToListAsync / CountAsync / FirstAsync / FirstOrDefaultAsync を提供する。
  CancellationToken をコマンドと読み取りに渡す。
- ExecuteUpdate / ExecuteDelete と非同期版は、条件付きの単一テーブルを対象とする。
  値の計算にも共通の式変換を使う。
- クエリ計画は上限256件の LRU キャッシュを共有する。毎回実行時の値を読み取り、
  接続・クロージャ・パラメータ値をキャッシュしない。

## 制約と互換性の変更

- Reverse / Last / LastOrDefault は明示的な OrderBy が必要。主キーへの暗黙の並び替えは行わない。
  並び順を指定しない通常のクエリに結果順序の保証はない。
- 範囲外の ElementAt は ArgumentOutOfRangeException、ElementAtOrDefault は default(T)。
- OrderBy を重ねると並び順を置き換える。追加のキーには ThenBy を使う。
- nullable の == / !=、その否定、Contains は C# の null の扱いに合わせる。
- SQL に変換できない行ごとの処理は最終 Select 内でも NotSupportedException。
  必要な処理は AsEnumerable() 以降で明示する。コンストラクターは結果構築に使用できるが、
  任意のコンストラクターの内部処理を推測して、そのプロパティ参照を SQL へ展開することはしない。
- 結合・集合演算・サブクエリは同じ接続の Kuery クエリ同士に限定する。
  集合演算の左右は同じ結果形状が必要。MemberInit の代入順は左右で異なっていてもよい。
- GroupBy の IGrouping 自体の読み取り、相関 SelectMany、比較子付きの演算、
  インデックス付きの Where / Select、Index / Range 指定のページングは未対応。
- 一括更新・削除では条件なし、JOIN、GroupBy、Distinct、ソート、ページング、射影を拒否する。
  更新・削除に含まれるサブクエリには、各 DB 固有の対象テーブル参照の制約も適用される。
- Include、関連マッピング、CTE、ウィンドウ関数、
  SkipWhile / TakeWhile / Zip / Aggregate などの汎用処理は対象外。
- 生 SQL・通常の CRUD・マッピング属性・従来の Table<T>() は今回の再設計の対象外。


## 追加した外部結合・型演算の使用例

```cs
var pairs = connection.Query<Product>()
    .LeftJoin(connection.Query<Category>(), p => p.CategoryId, c => c.Id,
        (p, c) => new { p.Name, CategoryName = c == null ? null : c.Name });

var both = connection.Query<Product>()
    .FullJoin(connection.Query<ProductArchive>(), p => p.Id, a => a.Id,
        (p, a) => new { Current = p, Archived = a });

var ids = connection.Query<Product>().Where(p => p.Id < 0)
    .Select(p => p.Id).DefaultIfEmpty(); // 空の場合だけ0を1件返す
```

LeftJoin / RightJoin は .NET 10 の Queryable API を使う。FullJoin は
`KueryQueryableExtensions.FullJoin` として左右の IQueryable、キーと結果セレクターを受け取る。
利用環境に標準 Queryable.FullJoin がある場合は、結果セレクター付きの標準形と null 比較子も同じ経路で変換する。
比較子を指定した独自の等値比較は拒否する。

RightJoin は左右を入れ替えた LEFT JOIN、SQLite / MySQL の FullJoin は
LEFT JOIN と右側の未一致行の UNION ALL に変換する。PostgreSQL / SQL Server は FULL OUTER JOIN を使う。
存在マーカーで未一致を判定するため、重複と全列 null の一致行を保持する。

DefaultIfEmpty は引数なしのみ対応する。参照型は null、値型は default(T) を返す。
外部結合も未一致側の値全体を既定値にする。
Cast は結果構築式の型変換であり、型の絞り込みは行わない。
数値変換は EF Core の Expression.Convert に合わせる。型で絞り込む場合は OfType を使う。

## 読み取り専用 TPH

```cs
[Table("animals")]
[Discriminator("Kind")]
[DerivedType(typeof(Animal), "animal")]
[DerivedType(typeof(Cat), "cat")]
public class Animal
{
    [PrimaryKey]
    public int Id { get; set; }
    public string Name { get; set; }
}

public class Cat : Animal
{
    public int Lives { get; set; }
}

var animals = connection.Query<Animal>().ToList(); // 実際の CLR 型で構築
var cats = connection.Query<Animal>().OfType<Cat>().Where(c => c.Lives > 0);
var sameTable = connection.Query<Cat>(); // animals テーブルに型条件を適用
```

テーブル、文字列の型識別列、全派生型の列と識別値は利用側で用意する。
派生型専用の列は他の型の行で null を保持できるようにする。
抽象基底型も使用でき、全具象型を基底型の DerivedType 属性へ登録する。
具象基底型は自分自身も登録し、各具象型は公開の引数なしコンストラクターを必要とする。
型識別値の一意性と大小文字の比較には DB の照合規則も考慮する。
型識別列は通常の string プロパティとして明示的にマッピングしても、プロパティを設けなくてもよい。

OfType は対象型と登録された子孫型を取得する。未登録型・不整合なマッピングは NotSupportedException、
未登録の識別値をエンティティとして読み取った場合は InvalidOperationException とする。
既存の CRUD / Table / CreateTable には TPH の自動処理を追加しない。
TPH の ExecuteUpdate / ExecuteDelete は拒否し、TPT / TPC は対象外とする。

[40種類の集計方法と検証状況](query-operator-coverage.md)も参照。

エンティティ Contains では主キーなし・複数主キーのマッピングを拒否する。匿名型などの任意の構造比較は対象外。
