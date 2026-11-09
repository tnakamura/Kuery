using System;
using System.Data;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal static class SqlDialectFactory
    {
        internal static ISqlDialect Create(IDbConnection connection)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));

            if (connection.IsSqlServer())
            {
                return new SqlServerDialect();
            }

            if (connection.IsPostgreSql())
            {
                return new PostgreSqlDialect();
            }

            if (connection.IsMySql())
            {
                return new MySqlDialect();
            }

            if (connection.IsSqlite()) return new SqliteDialect();
            throw new NotSupportedException("Unsupported database connection: " + connection.GetType().FullName);
        }
    }
}
