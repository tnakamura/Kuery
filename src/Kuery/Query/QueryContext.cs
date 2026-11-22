using System;
using System.Data;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Carries the database connection used by a query execution.</summary>
    internal sealed class QueryContext
    {
        /// <summary>Creates an execution context for the supplied database connection.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <exception cref="System.ArgumentNullException">The connection is null.</exception>
        internal QueryContext(IDbConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <summary>Gets the database connection used to create commands.</summary>
        internal IDbConnection Connection { get; }
    }
}
