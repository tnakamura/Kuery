# EF Core のクエリ実装への追従

EF Core を読んだ際に Kuery 側の対応箇所を見つけやすくし、改善を取り込む際の変更範囲を明確にする。
比較の起点は、ローカルの dotnet/efcore リポジトリのコミット `16f8a8be5a`。
公開 API と .NET Standard 2.0 は維持し、EF Core パッケージへの依存は追加しない。

## 配置と対応

EF Core の `Query` / `Query.Internal` / `Query.SqlExpressions` に合わせて、
Kuery でも `src/Kuery/Query/`、`Query/Internal/`、`Query/SqlExpressions/` を使う。
各 SQL 式は原則として同名のファイルに配置する。

以下の EF Core パスは同リポジトリの `src/` を基準とする。

| 役割 | EF Core | Kuery |
|---|---|---|
| 全体の調整・キャッシュ検索 | EFCore/Query/Internal/QueryCompiler.cs | Query/Internal/QueryCompiler.cs |
| 実行時パラメータの抽出 | EFCore/Query/Internal/ExpressionTreeFuncletizer.cs | Query/Internal/ExpressionTreeFuncletizer.cs |
| 正規化 | EFCore/Query/QueryTranslationPreprocessor.cs | Query/QueryTranslationPreprocessor.cs |
| コンパイルの段階管理 | EFCore/Query/QueryCompilationContext.cs | Query/QueryCompilationContext.cs |
| LINQ 演算子の変換 | EFCore.Relational/Query/RelationalQueryableMethodTranslatingExpressionVisitor.cs | Query/RelationalQueryableMethodTranslatingExpressionVisitor.cs |
| スカラー式の変換 | EFCore.Relational/Query/RelationalSqlTranslatingExpressionVisitor.cs | Query/RelationalSqlTranslatingExpressionVisitor.cs |
| SQL と結果形状の組 | EFCore/Query/ShapedQueryExpression.cs | Query/ShapedQueryExpression.cs |
| 結果構築のコンパイル | EFCore.Relational/Query/RelationalShapedQueryCompilingExpressionVisitor.cs | Query/RelationalShapedQueryCompilingExpressionVisitor.cs |
| SQL 式の生成 | EFCore.Relational/Query/SqlExpressionFactory.cs | Query/SqlExpressionFactory.cs |
| SQL 式と SELECT | EFCore.Relational/Query/SqlExpressions/ | Query/SqlExpressions/ |
| SQL の走査 | QuerySqlGenerator / SqlNullabilityProcessor の VisitExtension | Query/SqlExpressionVisitor.cs |
| null 補償 | EFCore.Relational/Query/SqlNullabilityProcessor.cs | Query/SqlNullabilityProcessor.cs |
| SQL 生成 | EFCore.Relational/Query/QuerySqlGenerator.cs | Query/QuerySqlGenerator.cs |
| コンパイル済みクエリ | EFCore/Query/Internal/CompiledQueryCache.cs | Query/Internal/CompiledQueryCache.cs |
| キャッシュキー | EFCore/Query/CompiledQueryCacheKeyGenerator.cs | Query/Internal/CompiledQueryCacheKeyGenerator.cs |
| SQL コマンドの雛形 | EFCore.Relational/Query/Internal/RelationalCommandCache.cs | Query/Internal/RelationalCommandCache.cs |

Kuery の公開 KueryQueryProvider / KueryQueryable は、EF Core の EntityQueryProvider / EntityQueryable に近い入口であり、
既存 API の互換性のため名前を維持する。ADO.NET の実行と破棄は Internal/KueryLinqExecutor に残す。

## 実装を確認する順序

1. EF Core の変更が、パラメータ抽出、LINQ 変換、SQL 式変換、null 補償、SQL 生成、結果構築のどの段階に属するか確認する。
2. LINQ 演算子の改善は対応する TranslateWhere / TranslateSelect / TranslateOrderBy / TranslateThenBy / TranslateAny / TranslateAll などを比較する。
   集約は TranslateAggregateWithSelector、要素取得は TranslateSingleResult、集合演算は TranslateSetOperation に共通処理をまとめている。
3. 文字列・数学・日時関数の改善は RelationalSqlTranslatingExpressionVisitor の VisitMethodCall / VisitMember と、対応する SqliteDialect / PostgreSqlDialect / SqlServerDialect / MySqlDialect の Function を確認する。
4. SQL 式を書き換える場合は SqlExpressionVisitor と各型の Update を使う。キャッシュ済みの式を直接変更しない。
   SelectExpression は変換中には可変だが、VisitChildren による書き換えは変更された枝だけを新しい式へコピーする。
5. 対象の動作の回帰テストを追加し、SQL とパラメータ、SQLite の結果、外部3 DB の共通契約を検証する。
   null・空集合・演算順序・キャッシュ再利用への影響を含め、必要に応じてベンチマークを比較する。

## 意図的に異なる点

- EF Core のモデル、型マッピングサービス、ナビゲーション展開、変更追跡、DI コンテナーと各種 Factory は導入しない。
  QueryCompilationContext などの段階は保持し、必要な実装を直接構築する。
- EF Core が各 Visitor の VisitExtension で行う SQL 型ごとの振り分けは、Kuery の SqlExpressionVisitor に共通化する。
  VisitSqlBinary / VisitSqlUnary / VisitCase / VisitExists などの処理名は EF Core に合わせる。
- LINQ は標準の Queryable メソッドを扱う。EF Core 専用の拡張メソッドやモデル情報を使う変換は、そのまま移植しない。
- CLR 関数の登録用 Provider / Plugin 群は導入していない。対応範囲が小さいため、共通のスカラー Visitor と方言で実装する。
  SqlFunctionExpression の一部は `String.Length` のような方言から独立した意味名を保持する。
- SQL 型マッピングは CLR 型、null 許容性、既存 TableMapping と方言の範囲に限定する。
  EntityProjectionExpression、NonQueryExpression、CompiledQuery は Kuery に必要な最小限の情報を持つ。
- ResultCardinality は Enumerable / Single / SingleOrDefault に揃えるが、First / Single / ElementAt の検証には追加の QueryResultKind を保持する。
- SqlBinaryExpression の UseRelationalNulls は、JOIN の生の SQL 比較や補償済みの比較を再び補償しないための印であり、EF Core のオプションサービスとは別物。
- コンパイル済みクエリは上限256件の LRU。SQL 雛形は CompiledQuery のスロット情報だけを共有し、値や接続を保持しない。

既存動作と公開 API を守るため、EF Core の変更は対応する責務へ個別に取り込む。
同名の内部型であっても、EF Core と型・引数・内部契約が互換であることは意味しない。
