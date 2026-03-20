#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.test.yml"
SQL_PASSWORD="${KUERY_TEST_SQLSERVER_PASSWORD:-Kuery_test_123!}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found. Install Docker to run integration tests." >&2
  exit 1
fi

export KUERY_TEST_PG_HOST="localhost"
export KUERY_TEST_PG_PORT="54329"
export KUERY_TEST_PG_USERNAME="${KUERY_TEST_PG_USERNAME:-postgres}"
export KUERY_TEST_PG_PASSWORD="${KUERY_TEST_PG_PASSWORD:-postgres}"
export KUERY_TEST_PG_MASTER_DB="${KUERY_TEST_PG_MASTER_DB:-postgres}"

export KUERY_TEST_SQLSERVER_HOST="localhost,14333"
export KUERY_TEST_SQLSERVER_INTEGRATED_SECURITY="false"
export KUERY_TEST_SQLSERVER_USERNAME="${KUERY_TEST_SQLSERVER_USERNAME:-sa}"
export KUERY_TEST_SQLSERVER_PASSWORD="${KUERY_TEST_SQLSERVER_PASSWORD:-${SQL_PASSWORD}}"
export KUERY_TEST_SQLSERVER_MASTER_DB="${KUERY_TEST_SQLSERVER_MASTER_DB:-master}"

export KUERY_TEST_MYSQL_HOST="localhost"
export KUERY_TEST_MYSQL_PORT="33060"
export KUERY_TEST_MYSQL_USERNAME="${KUERY_TEST_MYSQL_USERNAME:-root}"
export KUERY_TEST_MYSQL_PASSWORD="${KUERY_TEST_MYSQL_PASSWORD:-mysql}"
export KUERY_TEST_MYSQL_MASTER_DB="${KUERY_TEST_MYSQL_MASTER_DB:-mysql}"

cleanup() {
  docker compose -f "${COMPOSE_FILE}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "${COMPOSE_FILE}" up -d --wait

cd "${REPO_ROOT}"
dotnet test test/Kuery.Tests/Kuery.Tests.csproj --filter "(FullyQualifiedName~Kuery.Tests.SqlClient|FullyQualifiedName~Kuery.Tests.Npgsql|FullyQualifiedName~Kuery.Tests.MySql)" "$@"
