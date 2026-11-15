# NuGet パッケージの公開

[Release NuGet Package](../.github/workflows/release-nuget.yml) は GitHub Release の
`published` イベントで実行されます。タグの push だけでは公開しません。
タグとリリースノートを準備してから公開でき、正式リリースとプレリリースの両方に対応します。

## 初回の設定

認証には [NuGet Trusted Publishing](https://learn.microsoft.com/en-us/nuget/nuget-org/trusted-publishing)
を使用します。長期 API キーや `NUGET_API_KEY` シークレットの登録は不要です。

1. `Kuery` パッケージの公開権限を持つアカウントで NuGet.org にログインし、
   **Trusted Publishing** から GitHub 用のポリシーを作成します。
   - Repository Owner: `tnakamura`
   - Repository: `Kuery`
   - Workflow File: `release-nuget.yml`（ディレクトリは含めません）
   - Environment: 空欄（このワークフローでは GitHub Environment を使用しません）
   - パッケージの対象を `Kuery` に限定し、新バージョンの公開を許可します。
2. GitHub リポジトリの **Settings → Secrets and variables → Actions → Variables** に
   `NUGET_USER` を登録します。値はポリシーを作成した NuGet.org のユーザー名です
   （メールアドレスや GitHub のユーザー名ではありません）。
3. このワークフローの変更をデフォルトブランチにマージします。

NuGet.org のポリシーが一時的に有効な状態の場合は、有効期間内に初回の公開を行ってください。
期限切れの場合はポリシーを再度有効にしてから実行します。

## リリース手順

1. 公開用ワークフローの変更を含むコミットに `v0.2.0` のようなタグを作成して GitHub に push します。

   ```bash
   git tag -a v0.2.0 -m "Release v0.2.0"
   git push origin v0.2.0
   ```

2. GitHub の **Releases → Draft a new release** でそのタグを選択し、
   リリースノートを記入して **Publish release** を実行します。
   下書きの保存だけではワークフローは実行されません。
3. GitHub Actions の **Release NuGet Package** が成功し、
   [NuGet.org](https://www.nuget.org/packages/Kuery) に新バージョンが表示されることを確認します。
   NuGet.org の検証・インデックス作成には時間がかかる場合があります。

タグは `vMAJOR.MINOR.PATCH` または `vMAJOR.MINOR.PATCH-prerelease` の形式にします。
プレリリースの例は `v0.2.0-preview.1` です。GitHub Release のプレリリース指定も行ってください。
NuGet のプレリリース判定はタグのサフィックスで決まります。
`v` を取り除いた値をビルドと pack の `Version` に渡すため、
公開のために `.csproj` のバージョンを更新する必要はありません。

ワークフローはリリース対象のコミットをチェックアウトし、Release 構成でビルドします。
SQLite と Docker 上の PostgreSQL・SQL Server・MySQL のテストが成功した後、
`Kuery` の `.nupkg` と `.snupkg` を生成します。
`dotnet nuget push` は `.nupkg` と対応するシンボルパッケージを NuGet.org に送信します。

失敗した実行は Actions から再実行できます。`--skip-duplicate` により公開済みバージョンは
スキップされます。公開済みパッケージの内容は置き換えられないため、修正を配布するときは
新しいバージョンのタグと Release を作成してください。

タグの形式が不正な場合や `NUGET_USER` が未設定の場合は、ビルド前に失敗します。
認証に失敗した場合は、NuGet.org のポリシーの所有者・対象パッケージ・ワークフロー名と
`NUGET_USER` を確認し、設定を修正してから再実行してください。
