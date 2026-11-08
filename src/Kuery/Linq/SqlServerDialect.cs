using System;

namespace Kuery.Linq
{
    internal sealed class SqlServerDialect : SqlDialect
    {
        public override SqlDialectKind Kind => SqlDialectKind.SqlServer;

        public override string EscapeIdentifier(string identifier)
        {
            if (identifier == null) throw new ArgumentNullException(nameof(identifier));
            return "[" + identifier.Replace("]", "]]") + "]";
        }

        public override string FormatParameterName(string parameterName)
        {
            if (parameterName == null) throw new ArgumentNullException(nameof(parameterName));
            return "@" + parameterName;
        }

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
