# クエリパイプラインの性能検証

2026-11-08 に .NET SDK 10.0.201 / macOS arm64 / Microsoft.Data.Sqlite 10.0.3 で測定した。
比較元は `2e104ea`、比較先は今回のクエリパイプライン。
EF Core や他の ORM との性能比較ではない。

## 同じ公開 API による変更前後の比較

`benchmark/QueryPipelineProbe` を両方のライブラリに対して実行した。
インメモリ SQLite に2テーブル各1000行を作成し、各クエリは10行を返す。
100回ウォームアップ後、3000回の平均時間とスレッド上の割り当て量を測定する。
両方とも `DOTNET_TieredCompilation=0`、Release ビルドを使用する。

| クエリ | 変更前 | 変更後 | 変更前の割り当て | 変更後の割り当て |
|---|---:|---:|---:|---:|
| エンティティ取得 | 8.89 μs | 12.10 μs | 5,192 B | 6,649 B |
| 匿名型への2列の射影 | 42.69 μs | 15.09 μs | 13,183 B | 8,865 B |
| 2テーブルの JOIN と射影 | 55.41 μs | 25.08 μs | 18,450 B | 11,538 B |

射影は約65%、JOIN は約55%短縮した。この測定では、単純なエンティティ取得は
約36%遅く、割り当て量も約28%増えている。
射影と JOIN の行ごとの動的呼び出し・中間オブジェクト作成は削減できたが、
単純な取得には正規化と構造キーによる計画検索の追加コストが残る。
すべてのクエリが高速化したという結果ではない。

最初のエンティティ取得は変更前21.775 ms / 変更後34.492 msだった。
これは JIT と初回の計画作成を含む。続く別の射影は6.111 ms / 2.174 ms、
JOIN は6.884 ms / 4.970 msであり、最初のクエリと JIT 状態が異なる。
これらの一回の値だけでコンパイル速度や一般的な初回レイテンシーは判断しない。
外部 DB の通信コスト、統計的な誤差幅、長時間の負荷はこの補助プローブの対象外。

再実行例:

```sh
DOTNET_TieredCompilation=0 dotnet run -c Release --project benchmark/QueryPipelineProbe
DOTNET_TieredCompilation=0 dotnet run -c Release --project benchmark/QueryPipelineProbe -p:KuerySource=/absolute/path/to/baseline
```

比較元には元の `src/Kuery` とパッケージ用 README を含むチェックアウトを指定する。
参照先を切り替えると依存関係の復元が必要になるため、この比較コマンドでは `--no-restore` を指定しない。

## BenchmarkDotNet

`benchmark/KueryBenchmark/QueryablePipeline.cs` には次の測定を追加した。
実行ランタイムは .NET 10、アーキテクチャはホストに合わせる。

- FirstExecution: 各イテレーションの前に計画キャッシュを消去して実行する。
- RepeatedExecution: 同じ構造のエンティティ取得を繰り返す。
- Count: 条件付き集約。
- Projection: SQL の計算式を含むスカラー射影。
- Join: 自己結合から必要な列だけを取得する。

```sh
dotnet run -c Release --project benchmark/KueryBenchmark -- --filter '*QueryablePipeline*'
```

MemoryDiagnoser で割り当て量も収集する。
FirstExecution は計画キャッシュの消去を測定の外で行い、プロセス起動時の JIT は含まない。
他のケースは計画と、利用可能な場合は SQL 雛形を再利用する。

通常の別プロセスのジョブで得た今回の結果は次のとおり。
ランタイムの階層化コンパイルは既定値を使うため、上の比較プローブとは測定条件が異なる。

| ケース | 平均 | 割り当て |
|---|---:|---:|
| FirstExecution | 172.891 μs | 28.04 KB |
| RepeatedExecution | 8.974 μs | 6.93 KB |
| Count | 7.472 μs | 5.00 KB |
| Projection | 7.067 μs | 7.72 KB |
| Join | 13.604 μs | 9.68 KB |

## 検証範囲

この環境では SQLite の実行テストと4方言の SQL 生成テストを実行した。
PostgreSQL・SQL Server・MySQL の共通実行テストも追加したが、Docker が存在せず、
`test/run-integration-tests.sh` は DB 起動前に終了した。
外部3 DB の実行結果と性能は、この記録では検証済みとして扱わない。
