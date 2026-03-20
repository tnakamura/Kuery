# MySQL 対応 実装計画

## 背景
- 現在の Kuery は SQLite / SQL Server / PostgreSQL をサポート。
- 既存3方言で提供している機能を、MySQL でも同等に使えるようにする。
- 本計画は **最小差分で既存設計に合わせる** ことを目的にする。

## 対象範囲
1. **コア SQL ビルド層**
   - 接続種別判定
   - 識別子エスケープ
   - パラメータ命名
   - Last Insert ID
   - InsertOrReplace（Upsert）
2. **IQueryable(LINQ) 層**
   - 方言選択
   - ページング差分
   - 文字列/数学/日時/型変換の方言差分
3. **既存 SqlHelper(TableQuery) 層**
   - SQL 生成分岐
   - 文字列比較差分
4. **テスト基盤**
   - MySQL Fixture
   - 既存テストパターン準拠の MySQL テスト追加
   - Docker 統合テストへの組み込み
5. **ドキュメント**
   - README のテスト実行手順と環境変数更新

## 実装ステップ詳細

### Step 1: 方言の土台追加
- `SqlDialectKind` に `MySql` を追加。
- `MySqlDialect` を追加（識別子: `` `name` ``, パラメータ: `@p1`）。
- `SqlDialectFactory` で `connection.IsMySql()` 判定を追加。

### Step 2: SqlBuilder に MySQL 分岐追加
- `IsMySql(IDbConnection)` 実装（`MySqlConnector.MySqlConnection` 判定）。
- `EscapeLiteral` で MySQL はバッククォートを利用。
- `CreateLastInsertRowIdCommand` を `SELECT LAST_INSERT_ID();` に対応。
- `CreateInsertOrReplaceCommand` に MySQL 分岐を追加し、
  `INSERT ... ON DUPLICATE KEY UPDATE ...` を生成。

### Step 3: LINQ SQL 生成(MySQL)対応
- `SelectSqlGenerator` のページングで
  `OFFSET` 単体時に `LIMIT 18446744073709551615` を付与。
- `SqlPredicateTranslator` で MySQL 向け分岐を追加:
  - 文字列連結: `concat(a, b)`
  - `StartsWith/EndsWith` (IgnoreCase): `LIKE concat(...)`
  - 日時: `NOW()`, `UTC_TIMESTAMP()`, `DATE_ADD`, `DAYOFWEEK`
  - 型変換: MySQL向け CAST 型名 (`double`, `bigint`, `unsigned`, `char`)
  - 数学関数: `FLOOR/CEILING/LOG/LOG10`

### Step 4: SqlHelper(TableQuery)の MySQL 対応
- `GenerateCommand` の方言分岐に MySQL 追加。
- MySQL 用コマンド生成を追加（LIMIT/OFFSET 仕様対応）。
- `StartsWith/EndsWith` IgnoreCase で `concat` 構文を利用。

### Step 5: テスト基盤追加
- `MySqlConnector` をテストプロジェクトに追加。
- `MySqlFixture` を新規追加:
  - DB 作成/破棄
  - 接続作成
  - 初期テーブル作成
  - 環境変数オーバーライド
- `TestHelper` に MySQL の `DropTable` 拡張追加。

### Step 6: MySQL テスト追加
- 既存 SQLite/SqlClient/Npgsql のスタイルを踏襲して追加:
  - `SqlBuilderTest`（パラメータ接頭辞/エスケープ/パラメータ化）
  - `QueryTest`（基本 Query マッピング）
  - `LinqTest`（LINQ 実行の代表ケース）
  - `SqlHelperTest`（IgnoreCase 文字列比較の方言差分）

### Step 7: 統合実行への組み込み
- `test/docker-compose.test.yml` に MySQL サービス追加。
- `test/run-integration-tests.sh` に MySQL 環境変数とテストフィルタを追加。
- README の integration 対象DBと環境変数を更新。

## 検証計画
1. 変更前後で `dotnet build` の成功を確認。
2. 追加テスト中心にターゲット実行:
   - `FullyQualifiedName~Kuery.Tests.MySql`
3. 既存高速テスト（SQLite）の回帰確認:
   - `./test/run-fast-tests.sh`
4. Docker 起動可能環境では integration 実行:
   - `./test/run-integration-tests.sh`

## 完了条件
- MySQL 接続で Kuery の主要機能（既存3方言相当）が実行可能。
- LINQ/SqlHelper/SqlBuilder の方言差分がコンパイル・テストで確認済み。
- MySQL用ユニットテストが追加され、既存テストを壊していない。
- README とテストスクリプトが MySQL を含む運用に更新されている。
