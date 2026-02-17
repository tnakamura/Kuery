using System;

namespace Kuery.Linq
{
    internal sealed class SqlServerDialect : ISqlDialect
    {
        public SqlDialectKind Kind => SqlDialectKind.SqlServer;

        public string EscapeIdentifier(string identifier)
        {
            if (identifier == null) throw new ArgumentNullException(nameof(identifier));
            return $"[{identifier}]";
        }

        public string FormatParameterName(string parameterName)
        {
            if (parameterName == null) throw new ArgumentNullException(nameof(parameterName));
            return "@" + parameterName;
        }
    }
}
