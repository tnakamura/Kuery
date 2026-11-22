using System;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Applies PostgreSQL syntax and function translations to relational SQL.</summary>
    internal sealed class PostgreSqlDialect : SqlDialect
    {
        /// <summary>Gets the database dialect that these SQL rules target.</summary>
        public override SqlDialectKind Kind => SqlDialectKind.PostgreSql;

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
            return "@" + parameterName;
        }

        /// <summary>Gets the SQL type name used to cast a value to the specified CLR type.</summary>
        /// <param name="type">The target CLR type of the cast.</param>
        /// <returns>The SQL type name for the cast.</returns>
        public override string CastType(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (type == typeof(bool)) return "boolean";
            if (type == typeof(double)) return "double precision";
            return base.CastType(type);
        }

        /// <summary>Renders a supported database function or equivalent SQL expression.</summary>
        /// <param name="operation">The logical SQL operation name.</param>
        /// <param name="a">The rendered SQL arguments in call order.</param>
        /// <returns>The SQL fragment that implements the operation.</returns>
        public override string Function(string operation, string[] a)
        {
            switch (operation)
            {
                case "String.Position": return "strpos(" + a[0] + ", " + a[1] + ")";
                case "String.Substring": return "substr(" + a[0] + ", cast((" + a[1] + " + 1) as integer)" + (a.Length == 3 ? ", cast(" + a[2] + " as integer)" : "") + ")";
                case "DateTime.Now": return "localtimestamp";
                case "DateTime.UtcNow": return "(now() at time zone 'UTC')";
                case "DateTime.Today": return "date_trunc('day', localtimestamp)";
                case "DateTime.Date": return "date_trunc('day', " + a[0] + ")";
                case "Math.Log10": return "log(" + a[0] + ")";
                case "Math.Round": return a.Length == 1 ? "round(cast(" + a[0] + " as numeric))" : "round(cast(" + a[0] + " as numeric), " + a[1] + ")";
            }
            if (operation.StartsWith("DateTime.Add", StringComparison.Ordinal))
                return "(" + a[0] + " + (" + a[1] + " * interval '1 " + operation.Substring(12).ToLowerInvariant() + "'))";
            if (operation.StartsWith("DateTime.", StringComparison.Ordinal))
                return "cast(extract(" + (operation == "DateTime.DayOfWeek" ? "dow" : operation.Substring(9).ToLowerInvariant()) + " from " + a[0] + ") as integer)";
            return base.Function(operation, a);
        }
    }
}
