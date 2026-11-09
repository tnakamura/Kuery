using System;
using System.Data;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class QueryContext
    {
        internal QueryContext(IDbConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        internal IDbConnection Connection { get; }
    }
}
