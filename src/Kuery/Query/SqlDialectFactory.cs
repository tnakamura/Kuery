using System;
using System.Data;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Selects SQL rendering rules for a supported connection provider.</summary>
    internal static class SqlDialectFactory
    {
        /// <summary>Selects a dialect from the supplied ADO.NET connection.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <returns>The SQL dialect for the connection.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="connection"/> is null.</exception>
        /// <exception cref="System.NotSupportedException">The connection type is not a supported provider.</exception>
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
