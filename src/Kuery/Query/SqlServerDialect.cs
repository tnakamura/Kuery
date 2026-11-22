using System;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Applies SQL Server syntax and function translations to relational SQL.</summary>
    internal sealed class SqlServerDialect : SqlDialect
    {
        /// <summary>Gets the database dialect that these SQL rules target.</summary>
        public override SqlDialectKind Kind => SqlDialectKind.SqlServer;

        /// <summary>Quotes a database identifier and escapes embedded identifier delimiters.</summary>
        /// <param name="identifier">The unquoted identifier.</param>
        /// <returns>The identifier quoted for the current dialect.</returns>
        public override string EscapeIdentifier(string identifier)
        {
            if (identifier == null) throw new ArgumentNullException(nameof(identifier));
            return "[" + identifier.Replace("]", "]]") + "]";
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
            if (type == typeof(string) || type == typeof(char)) return "nvarchar(max)";
            if (type == typeof(bool)) return "bit";
            if (type == typeof(double)) return "float";
            if (type == typeof(decimal)) return "decimal(38, 18)";
            if (type == typeof(DateTime)) return "datetime2";
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
                case "String.Length": return "(len(" + a[0] + " + N'#') - 1)";
                case "String.Position": return "charindex(" + a[1] + ", " + a[0] + ")";
                case "String.Substring": return "substring(" + a[0] + ", (" + a[1] + " + 1), " + (a.Length == 3 ? a[2] : "2147483647") + ")";
                case "String.Trim": return "ltrim(rtrim(" + a[0] + "))";
                case "String.Concat": return "(" + string.Join(" + ", System.Linq.Enumerable.Select(a, x => "coalesce(" + x + ", N'')")) + ")";
                case "DateTime.Now": return "getdate()";
                case "DateTime.UtcNow": return "getutcdate()";
                case "DateTime.Today": return "cast(cast(getdate() as date) as datetime2)";
                case "DateTime.Date": return "cast(cast(" + a[0] + " as date) as datetime2)";
                case "DateTime.DayOfWeek": return "(((datediff(day, '19000107', " + a[0] + ") % 7) + 7) % 7)";
                case "Math.Round": return "round(" + a[0] + ", " + (a.Length == 2 ? a[1] : "0") + ")";
                case "Math.Ceiling": return "ceiling(" + a[0] + ")";
                case "Math.Log": return "log(" + string.Join(", ", a) + ")";
            }
            if (operation.StartsWith("DateTime.Add", StringComparison.Ordinal))
                return "dateadd(" + operation.Substring(12).TrimEnd('s').ToLowerInvariant() + ", " + a[1] + ", " + a[0] + ")";
            if (operation.StartsWith("DateTime.", StringComparison.Ordinal)) return "datepart(" + operation.Substring(9).ToLowerInvariant() + ", " + a[0] + ")";
            return base.Function(operation, a);
        }
    }
}
