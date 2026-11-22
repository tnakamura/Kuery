using System;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Applies SQLite syntax and function translations to relational SQL.</summary>
    internal sealed class SqliteDialect : SqlDialect
    {
        /// <summary>Gets the database dialect that these SQL rules target.</summary>
        public override SqlDialectKind Kind => SqlDialectKind.Sqlite;

        /// <summary>Quotes a database identifier and escapes embedded identifier delimiters.</summary>
        /// <param name="identifier">The unquoted identifier.</param>
        /// <returns>The identifier quoted for the current dialect.</returns>
        public override string EscapeIdentifier(string identifier)
        {
            if (identifier == null) throw new ArgumentNullException(nameof(identifier));
            return "\"" + identifier.Replace("\"", "\"\"") + "\"";
        }

        /// <summary>Prefixes a parameter name as required by the database provider.</summary>
        /// <param name="parameterName">The parameter name without its provider prefix.</param>
        /// <returns>The provider-specific parameter name.</returns>
        public override string FormatParameterName(string parameterName)
        {
            if (parameterName == null) throw new ArgumentNullException(nameof(parameterName));
            return "$" + parameterName;
        }

        /// <summary>Renders a supported database function or equivalent SQL expression.</summary>
        /// <param name="operation">The logical SQL operation name.</param>
        /// <param name="a">The rendered SQL arguments in call order.</param>
        /// <returns>The SQL fragment that implements the operation.</returns>
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
