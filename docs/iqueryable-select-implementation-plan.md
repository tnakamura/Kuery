# IQueryable ベース SELECT 導入計画（Kuery）

## 目的
`connection.Query<T>()`（引数なし）を入口にして、LINQ の式木を SQL `SELECT` に変換・実行できるようにする。

想定利用例:

```cs
var products = await connection.Query<Product>()
    .Where(p => p.Code == "0001")
    .OrderBy(p => p.Code)
    .ToListAsync();
```

## 方針（改訂）
- API は `Query<T>()` の引数なしオーバーロードを追加する。
- `TableQuery<T>` の実装は再利用しない。
- `IQueryable<T>` 専用のクエリパイプラインを新規実装する。
- v1 の SQL 変換対象は以下に限定する。
  - `Where`
  - `OrderBy` / `ThenBy`
  - `Skip` / `Take`
  - `Count`
  - `First` / `FirstOrDefault`
- 非同期実行は `IQueryable<T>` 向けの独自拡張（`ToListAsync` など）で提供する。
- 未対応演算子は「クライアント評価へフォールバック」ではなく、原則 `NotSupportedException` を返して明示的に失敗させる（曖昧な動作を避ける）。

## 新アーキテクチャ（TableQuery 非依存）
1. **Query Model 層**
   - `SelectQueryModel`（テーブル、述語、ソート、ページング、終端演算）を定義。
   - `Expression` を直接 SQL へ変換せず、一度 Query Model に正規化する。

2. **Expression Translator 層**
   - `Queryable` の `MethodCallExpression` を順に解析して Query Model を構築。
   - `Where` の述語は専用 `PredicateTranslator` で SQL 条件式（`SqlFragment`）へ変換。
   - 変換時にパラメータ値を収集し、SQL 文字列と分離して保持。

3. **Dialect SQL Generator 層**
   - `ISqlDialect` を導入し、Sqlite / SqlServer / PostgreSQL を切替。
   - 識別子クオート、ページング構文、パラメータ名規約を方言へ集約。
   - `SelectQueryModel` + `SqlFragment` から最終 SQL + `IDbDataParameter` を生成。

4. **Execution 層**
   - 生成済み SQL を既存の低レベル実行ユーティリティ（Reader → Mapping）で実行。
   - `ToList` / `Count` / `First` / `FirstOrDefault` と async 対応を分離実装。

5. **Error Policy 層**
   - 非対応式は即時 `NotSupportedException`。
   - 例外メッセージに「未対応ノード」「対応済み演算子一覧」を含める。

## 実装ステップ
1. **入口 API の追加**
   - `SqlHelper` に `Query<T>()`（引数なし）を追加。
   - 既存 `Query<T>(string sql, object param = null)` は維持して互換性を確保する。

2. **`IQueryable<T>` / `IQueryProvider` の追加**
   - 新規 `KueryQueryable<T>`（`IQueryable<T>` 実装）を追加。
   - 新規 `KueryQueryProvider`（`IQueryProvider` 実装）を追加。
   - `Queryable` の `MethodCallExpression` を解析し、v1 対応演算子のみを `SelectQueryModel` へ反映する。

3. **Query Model / Translator の新規実装**
   - `SelectQueryModel`、`SqlFragment`、`ParameterBag` を追加。
   - `Where` 述語の式木変換を `PredicateTranslator` として新設。
   - `OrderBy` / `Skip` / `Take` / 終端演算を Query Model に集約。

4. **Dialect SQL Generator の新規実装**
   - `ISqlDialect` + `SqliteDialect` + `SqlServerDialect` + `PostgreSqlDialect` を追加。
   - Query Model から方言別 SQL を生成する `SelectSqlGenerator` を追加。

5. **非同期拡張の追加**
   - `IQueryable<T>` に対して `ToListAsync` / `CountAsync` / `FirstAsync` / `FirstOrDefaultAsync` を追加。
   - 内部では新しい Query Model + SQL Generator の実行経路を呼ぶ。

6. **エラーポリシーの実装**
   - 未対応の `Queryable` 演算子（例: `Join` / `GroupBy` / 複雑な `Select`）は明示例外。
   - 変換可能な式ノードだけを許可し、境界を明確化する。

7. **テスト追加（3 プロバイダ横断）**
   - Sqlite / SqlClient / Npgsql で同等テストを追加。
   - `Query<T>()` 経由で `Where/OrderBy/Skip/Take/Count/First/FirstOrDefault` が既存 `Table<T>()` と同じ結果になることを検証。
   - async 拡張の動作（`ToListAsync` など）を追加検証。
   - 非対応演算子で明示的に例外が発生することを検証。

8. **README 更新**
   - `Query<T>()` の利用例（同期/非同期）を追記。
   - v1 対応範囲と「未対応時は例外」の方針を明記する。

## 変更対象候補ファイル
- `src/Kuery/SqlHelper.cs`
- `src/Kuery/AsyncSqlHelper.cs`
- （新規）`src/Kuery/KueryQueryable.cs`
- （新規）`src/Kuery/KueryQueryProvider.cs`
- （新規）`src/Kuery/Linq/SelectQueryModel.cs`
- （新規）`src/Kuery/Linq/PredicateTranslator.cs`
- （新規）`src/Kuery/Linq/SelectSqlGenerator.cs`
- （新規）`src/Kuery/Linq/ISqlDialect.cs`
- （新規）`src/Kuery/Linq/SqliteDialect.cs`
- （新規）`src/Kuery/Linq/SqlServerDialect.cs`
- （新規）`src/Kuery/Linq/PostgreSqlDialect.cs`
- `test/Kuery.Tests/*`（Sqlite / SqlClient / Npgsql）
- `README.md`

## リスクと対策
- **DB 方言差分（SqlClient/Npgsql/Sqlite）**
  - 対策: 方言責務を `ISqlDialect` 配下に明確分離し、SQL 文字列生成を共通コードから排除する。
- **式木変換の複雑化**
  - 対策: v1 で対応する式ノードを明文化し、未対応は即時例外で境界を固定する。
- **既存 API との意味差**
  - 対策: `Query<T>()` の動作仕様（非対応時例外）を README とテストで明示し、`Table<T>()` とは責務を分離する。

## 完了条件（Definition of Done）
- `Query<T>()` + `IQueryable<T>` で v1 対応演算子が SQL 変換される。
- `await connection.Query<T>()...ToListAsync()` が利用できる。
- `Query<T>()` 実装が `TableQuery<T>` に依存しない（実装参照・委譲なし）。
- 追加テストが 3 プロバイダで通過する。
- README に新 API と制約が反映される。
