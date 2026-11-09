using System;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class SqliteDialect : SqlDialect
    {
        public override SqlDialectKind Kind => SqlDialectKind.Sqlite;

        public override string EscapeIdentifier(string identifier)
        {
            if (identifier == null) throw new ArgumentNullException(nameof(identifier));
            return "\"" + identifier.Replace("\"", "\"\"") + "\"";
        }

        public override string FormatParameterName(string parameterName)
        {
            if (parameterName == null) throw new ArgumentNullException(nameof(parameterName));
            return "$" + parameterName;
        }

        public override string Function(string operation, string[] a)
        {
            switch (operation)
            {
                case "DateTime.Now": return "datetime('now', 'localtime')";
                case "DateTime.UtcNow": return "datetime('now')";
                case "DateTime.Today": return "datetime('now', 'localtime', 'start of day')";
                case "DateTime.Date": return "datetime(" + a[0] + ", 'start of day')";
                case "DateTime.Year": return DatePart("%Y", a[0]);
                case "DateTime.Month": return DatePart("%m", a[0]);
                case "DateTime.Day": return DatePart("%d", a[0]);
                case "DateTime.Hour": return DatePart("%H", a[0]);
                case "DateTime.Minute": return DatePart("%M", a[0]);
                case "DateTime.Second": return DatePart("%S", a[0]);
                case "DateTime.DayOfWeek": return DatePart("%w", a[0]);
                case "Math.Floor": return "(cast(" + a[0] + " as integer) - (" + a[0] + " < cast(" + a[0] + " as integer)))";
                case "Math.Ceiling": return "(cast(" + a[0] + " as integer) + (" + a[0] + " > cast(" + a[0] + " as integer)))";
            }
            if (operation.StartsWith("DateTime.Add", StringComparison.Ordinal))
                return "datetime(" + a[0] + ", (" + a[1] + " || ' " + operation.Substring(12).ToLowerInvariant() + "'))";
            return base.Function(operation, a);
        }

        static string DatePart(string format, string value) => "cast(strftime('" + format + "', " + value + ") as integer)";
    }
}
