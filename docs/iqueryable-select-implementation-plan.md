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

## 方針（確定事項）
- API は `Query<T>()` の引数なしオーバーロードを追加する。
- v1 の SQL 変換対象は以下に限定する。
  - `Where`
  - `OrderBy` / `ThenBy`
  - `Skip` / `Take`
  - `Count`
  - `First` / `FirstOrDefault`
- 非同期実行は `IQueryable<T>` 向けの独自拡張（`ToListAsync` など）で提供する。
- 既存挙動との互換性を優先し、未対応演算子は可能な範囲でクライアント評価へフォールバックする。

## 実装ステップ
1. **入口 API の追加**
   - `SqlHelper` に `Query<T>()`（引数なし）を追加。
   - 既存 `Query<T>(string sql, object param = null)` は維持して互換性を確保する。

2. **`IQueryable<T>` / `IQueryProvider` の追加**
   - 新規 `KueryQueryable<T>`（`IQueryable<T>` 実装）を追加。
   - 新規 `KueryQueryProvider`（`IQueryProvider` 実装）を追加。
   - `Queryable` の `MethodCallExpression` を解析し、v1 対応演算子のみを内部クエリ状態へ反映する。

3. **既存 SQL 生成ロジックの再利用**
   - 既存 `TableQuery<T>` の式変換・SQL 生成を最大限利用する。
   - 必要最小限の可視性変更（`private` → `internal` 等）または委譲ポイント追加で、重複実装を避ける。

4. **非同期拡張の追加**
   - `IQueryable<T>` に対して `ToListAsync` / `CountAsync` / `FirstAsync` / `FirstOrDefaultAsync` を追加。
   - 内部では既存 async 実装へ委譲する。

5. **互換性ルールの定義**
   - 未対応の `Queryable` 演算子（例: `Join` / `GroupBy` / 複雑な `Select`）は、
     - 安全にフォールバック可能ならクライアント評価へ寄せる。
     - 不正確になる場合は明示例外（対応外メッセージ）を返す。

6. **テスト追加（3 プロバイダ横断）**
   - Sqlite / SqlClient / Npgsql で同等テストを追加。
   - `Query<T>()` 経由で `Where/OrderBy/Skip/Take/Count/First/FirstOrDefault` が既存 `Table<T>()` と同じ結果になることを検証。
   - async 拡張の動作（`ToListAsync` など）を追加検証。

7. **README 更新**
   - `Query<T>()` の利用例（同期/非同期）を追記。
   - v1 対応範囲と非対応演算子を明記する。

## 変更対象候補ファイル
- `src/Kuery/SqlHelper.cs`
- `src/Kuery/AsyncSqlHelper.cs`
- （新規）`src/Kuery/KueryQueryable.cs`
- （新規）`src/Kuery/KueryQueryProvider.cs`
- `test/Kuery.Tests/*`（Sqlite / SqlClient / Npgsql）
- `README.md`

## リスクと対策
- **DB 方言差分（SqlClient/Npgsql/Sqlite）**
  - 対策: 既存 `TableQuery<T>` の SQL 生成を再利用し、差分ロジックの二重実装を避ける。
- **既存 LINQ 利用との挙動差**
  - 対策: v1 は範囲を限定し、未対応時はフォールバック優先で破壊的変更を回避。
- **実装複雑化**
  - 対策: `Join/GroupBy/高度な投影` は v1 対象外とし、段階的リリースにする。

## 完了条件（Definition of Done）
- `Query<T>()` + `IQueryable<T>` で v1 対応演算子が SQL 変換される。
- `await connection.Query<T>()...ToListAsync()` が利用できる。
- 既存 `Table<T>()` 系テストが回帰しない。
- 追加テストが 3 プロバイダで通過する。
- README に新 API と制約が反映される。
