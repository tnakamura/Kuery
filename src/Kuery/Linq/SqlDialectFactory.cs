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

            return new SqliteDialect();
        }
    }
}
