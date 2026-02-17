using System;

namespace Kuery.Linq
{
    internal sealed class SqliteDialect : ISqlDialect
    {
        public SqlDialectKind Kind => SqlDialectKind.Sqlite;

        public string EscapeIdentifier(string identifier)
        {
            if (identifier == null) throw new ArgumentNullException(nameof(identifier));
            return $"[{identifier}]";
        }

        public string FormatParameterName(string parameterName)
        {
            if (parameterName == null) throw new ArgumentNullException(nameof(parameterName));
            return "$" + parameterName;
        }
    }
}
