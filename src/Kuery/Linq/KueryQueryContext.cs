using System;
using System.Data;

namespace Kuery.Linq
{
    internal sealed class KueryQueryContext
    {
        internal KueryQueryContext(IDbConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        internal IDbConnection Connection { get; }
    }
}
