using System;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class MySqlDialect : SqlDialect
    {
        public override SqlDialectKind Kind => SqlDialectKind.MySql;

        public override string EscapeIdentifier(string identifier)
        {
            if (identifier == null) throw new ArgumentNullException(nameof(identifier));
            return "`" + identifier.Replace("`", "``") + "`";
        }

        public override string FormatParameterName(string parameterName)
        {
            if (parameterName == null) throw new ArgumentNullException(nameof(parameterName));
            return "@" + parameterName;
        }

        public override string CastType(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (type == typeof(string) || type == typeof(char)) return "char";
            if (type == typeof(double) || type == typeof(float)) return "double";
            if (type == typeof(decimal)) return "decimal(38, 18)";
            if (type == typeof(DateTime)) return "datetime";
            return "signed";
        }

        public override string Function(string operation, string[] a)
        {
            switch (operation)
            {
                case "String.Length": return "char_length(" + a[0] + ")";
                case "String.Concat": return "concat(" + string.Join(", ", System.Linq.Enumerable.Select(a, x => "coalesce(" + x + ", '')")) + ")";
                case "DateTime.Now": return "now()";
                case "DateTime.UtcNow": return "utc_timestamp()";
                case "DateTime.Today": return "cast(curdate() as datetime)";
                case "DateTime.Date": return "cast(date(" + a[0] + ") as datetime)";
                case "DateTime.DayOfWeek": return "(dayofweek(" + a[0] + ") - 1)";
            }
            if (operation.StartsWith("DateTime.Add", StringComparison.Ordinal))
                return "date_add(" + a[0] + ", interval " + a[1] + " " + operation.Substring(12).TrimEnd('s').ToLowerInvariant() + ")";
            if (operation.StartsWith("DateTime.", StringComparison.Ordinal)) return operation.Substring(9).ToLowerInvariant() + "(" + a[0] + ")";
            return base.Function(operation, a);
        }
    }
}
