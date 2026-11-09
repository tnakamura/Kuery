# Kuery のクエリパイプライン

EF Core の正規化、SQL 式、ShapedQueryExpression、コンパイル済みクエリの分離を参考に、
Kuery の IQueryable 専用パイプラインを独立して実装する。
EF Core のパッケージや DbContext、変更追跡、ナビゲーションモデルは使用しない。

## 処理の流れ

`LINQ → QueryCompiler → QueryCompilationContext → ShapedQueryExpression → CompiledQuery → RelationalCommandCache → QuerySqlGenerator → KueryLinqExecutor`

1. QueryCompiler は ExpressionTreeFuncletizer で、値として評価できる部分を QueryParameterExpression に置き換える。
   キャプチャされたクエリも正規化し、接続とプロバイダーを検証する。
   DateTime.Now / UtcNow / Today は SQL の現在時刻式として保持する。
   .NET 10 の配列 Contains に含まれる ReadOnlySpan 変換もここで除く。
2. QueryCompilationContext は前処理・変換・結果構築のコンパイルを順に実行する。
   QueryTranslationPreprocessor は直接渡されたラムダの Invoke を展開する。
   RelationalQueryableMethodTranslatingExpressionVisitor は TranslateWhere / TranslateSelect / TranslateOrderBy / TranslateAny などで LINQ 演算子を処理する。
   ラムダ引数を直前の射影に束縛し、スカラー部分は RelationalSqlTranslatingExpressionVisitor へ渡す。
   演算順序を保つ必要がある箇所では派生テーブルを作り、列参照を新しいエイリアスへ束縛し直す。
3. SQL の列・パラメータ・定数・演算・関数・サブクエリは Query/SqlExpressions に配置する。
   SqlBinaryExpression / SqlUnaryExpression / SqlFunctionExpression / CaseExpression / InExpression / ExistsExpression などを使い、CLR 型と null 許容性を保持する。
   QueryParameterExpression はここで SqlParameterExpression へ変換する。
   文字列を後処理して列を修飾せず、スコープごとのテーブル参照で識別する。
   集合演算は各入力を独立した SELECT として保持し、結果のメンバー順を揃える。
4. ShapedQueryExpression は QueryExpression と ShaperExpression、結果の件数種別を保持する。
   RelationalShapedQueryCompilingExpressionVisitor は、射影と SELECT の列位置から IDataRecord 用のデリゲートを一度コンパイルし、CompiledQuery を作る。
   スカラー・匿名型・コンストラクター・MemberInit・エンティティを直接作る。
   LEFT JOIN の不在は派生テーブル内の存在マーカーで判定する。
5. RelationalCommandCache は SQL 雛形の再利用と値の束縛を担当する。
   SQL を作る場合は SqlNullabilityProcessor がパラメータ値に応じて null 比較を補償し、QuerySqlGenerator が方言別 SQL とパラメータ情報を生成する。
   SQL Server の真偽値射影、COUNT_BIG、ページングの0件取得などを処理する。
6. KueryLinqExecutor はコマンドとリーダーを破棄し、同じ結果構築処理を同期・非同期で使用する。
   First / Single 系は必要な行数で読み取りを終了する。
   非同期拡張は終端メソッドを式木に追加してから共通パイプラインへ渡す。

一括更新・削除も同じ正規化、スカラー式、方言、パラメータ処理を使う。
条件付き単一テーブルの検証は SQL 生成前に行い、無視された演算子によって対象が広がることを防ぐ。

## キャッシュ

- プロセス内で上限256件の LRU を共有する。追加・参照・追い出しは同じロックで保護する。
- キーは正規化した式の構造、型・メンバーの同一性、テーブルと列のマッピング、
  方言、結果型、実行種別、同期・非同期、更新対象と更新値の構造を含む。
  キー作成のバッファはスレッド単位で再利用し、式への参照は使用後に消去する。
- CompiledQueryCache は CompiledQuery を保持する。接続、元の式木のクロージャ、実際の値は保持しない。
- SQL の形が固定の場合はコマンド雛形も共有する。雛形にはパラメータ名・型・スロット番号だけを保存する。
  雛形の初期化は原子的に行い、毎回新しいコマンドへ値を束縛する。
- null による SQL の変更、コレクション Contains の展開、MySQL のページング、StringComparison による分岐などは、
  計画のみ再利用して実行ごとに SQL を生成する。
- キャッシュによって同じ接続の同時使用が安全になるわけではない。接続の利用規則は ADO.NET プロバイダーに従う。

## 検証方法

- QueryablePipelineTest は SQLite の実行結果を LINQ to Objects と比較する。
  固定シードのランダムな演算列、null、空集合、射影、自己結合、全列 null の LEFT JOIN、
  集合演算、相関集計、変更される値とコレクション、キャッシュの共有・追い出しを扱う。
- QueryPipelineContract は同じ動作を4 DB に適用する。SQLite は高速テスト、残りは既存の統合テストで実行する。
- QueryableSqlGenerationTest は DB 接続を開かずに4方言の SQL とパラメータ、識別子のエスケープを検証する。
- QueryableExpressionVisitorTest は JOIN・集合演算・相関サブクエリ内の SQL 式を書き換え、元のキャッシュ済みツリーが変化しないことと、Invoke の前処理を検証する。
- QueryablePipeline ベンチマークは .NET 10 の初回・反復実行、Count、計算式の射影、JOIN と割り当て量を測定する。

```sh
dotnet build Kuery.slnx
./test/run-fast-tests.sh --nologo
./test/run-integration-tests.sh --nologo
dotnet run -c Release --project benchmark/KueryBenchmark -- --filter '*QueryablePipeline*'
```

初回実行のベンチマークは各イテレーション前にキャッシュを消去する。
これはプロセス起動時間や初回 JIT を測るものではない。
対応範囲と互換性の変更は [SQL サポート状況](iqueryable-sql-support-status.md) を参照。
変更前後の測定と検証上の制約は [性能検証](query-pipeline-performance.md) に記録する。
EF Core の参照先、対応するクラス、意図的な相違は [EF Core への追従](efcore-query-alignment.md) を参照。
