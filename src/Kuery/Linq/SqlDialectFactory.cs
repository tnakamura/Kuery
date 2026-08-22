using System;
using System.Data;

namespace Kuery.Linq
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

            if (connection.IsSqlite())
            {
                return new SqliteDialect();
            }

            throw new NotSupportedException(
                $"Unsupported connection type '{connection.GetType().FullName}'. " +
                "Kuery's LINQ provider supports Microsoft.Data.SqlClient/System.Data.SqlClient, Npgsql, MySqlConnector, and Microsoft.Data.Sqlite/System.Data.Sqlite connections.");
        }
    }
}
