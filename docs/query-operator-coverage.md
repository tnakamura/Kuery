# Queryable 演算子の対応率

比較基準は EF Core のコミット `16f8a8be5a` を調べた際の40種類のメソッド名に、
同じコミットの前処理で扱う Order、OrderDescending、MinBy、MaxBy を加えた44種類。
実装済みの最終 GroupJoin は独立したパターンとして検証する。
全オーバーロードや EF Core 全機能との互換性を表す数値ではない。
`FullJoin` は比較対象には存在するが .NET 10 の標準 API にはないため、Kuery の拡張 API を提供する。

| 分類 | 演算子 | 種類数 |
|---|---|---:|
| 絞り込み・射影 | Where, Select | 2 |
| 並び順 | OrderBy, OrderByDescending, ThenBy, ThenByDescending, Reverse, Order, OrderDescending | 7 |
| ページング | Skip, Take | 2 |
| 重複排除 | Distinct | 1 |
| 集約 | Count, LongCount, Sum, Min, Max, Average, MinBy, MaxBy | 8 |
| 判定 | Any, All, Contains | 3 |
| 要素取得 | First, FirstOrDefault, Single, SingleOrDefault, Last, LastOrDefault, ElementAt, ElementAtOrDefault | 8 |
| 結合 | Join, LeftJoin, RightJoin, FullJoin, SelectMany | 5 |
| 集合演算 | Union, Concat, Intersect, Except | 4 |
| グループ化 | GroupBy | 1 |
| 型 | Cast, OfType | 2 |
| 既定値 | DefaultIfEmpty | 1 |
| 合計 | | 44 |

実行可能な一覧は `test/Kuery.Tests/QueryableOperatorCoverageTest.cs` の
`OperatorCoverageCases` と `AllSelectedOperatorFamiliesExecute` に置く。
同じ44ケースを QueryPipelineContract 経由で4 DB のテストへ展開する。

追加8種類の合成・境界条件は QueryableRemainingOperatorsTest、TPH は QueryableInheritanceTest、
方言ごとの FULL JOIN 生成とキャッシュ・GC・並行実行は SQLite の QueryableOperatorInfrastructureTest で検証する。
相関 SelectMany、グループ化、最終 GroupJoin、非同期と FromSql の実行は QueryableExtendedContractTest で検証する。
各方言の APPLY / LATERAL と診断 SQL・タグは QueryableOperatorInfrastructureTest でも検証する。

## 検証状況

44/44種類の変換と実行テストを実装した。SQLite の高速テスト555件と4方言の SQL 生成をローカルで検証した。
外部3 DB の共通シナリオは既存の integration CI で実行される。
今回のローカル環境には Docker がなく、統合テストは起動前に終了した。
4 DB の実行検証が完了するまでは「4 DB で100%検証済み」とは扱わない。

## 数えない機能と制約

AsQueryable はクエリを SQL に変換する演算ではないため分母に含めない。
GroupJoin は標準 LEFT JOIN パターンと最終演算子としての対応を別に記録する。
比較対象のリレーショナル変換で未対応の SkipWhile / TakeWhile も分母に含めない。

比較子付き、インデックス付き、Index / Range 指定などの全オーバーロードは対象外。
DefaultIfEmpty は引数なし、OfType はマッピングされたエンティティを対象とし、派生型の取得には TPH の登録を必要とする。
Cast は EF Core の結果構築式の変換に合わせる。数値型間で Expression.Convert が定義される場合は数値変換となり、
Enumerable.Cast のボックス化された値に対する厳密なアンボックスと異なる場合がある。
対応範囲の詳細は [SQL サポート状況](iqueryable-sql-support-status.md) を参照する。
