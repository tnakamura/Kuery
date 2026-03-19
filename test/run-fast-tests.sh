#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"
dotnet test test/Kuery.Tests/Kuery.Tests.csproj --filter "FullyQualifiedName~Kuery.Tests.Sqlite" "$@"
