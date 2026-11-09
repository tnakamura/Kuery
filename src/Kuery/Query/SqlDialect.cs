using System;
using System.Linq;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal abstract class SqlDialect : ISqlDialect
    {
        public abstract SqlDialectKind Kind { get; }
        public abstract string EscapeIdentifier(string identifier);
        public virtual string FormatParameterName(string parameterName) => "@" + parameterName;

        public virtual string CastType(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (type.IsEnum) type = Enum.GetUnderlyingType(type);
            if (type == typeof(string) || type == typeof(char)) return "text";
            if (type == typeof(bool)) return "integer";
            if (type == typeof(float) || type == typeof(double)) return "real";
            if (type == typeof(decimal)) return "numeric";
            if (type == typeof(DateTime)) return "timestamp";
            if (type == typeof(long)) return "bigint";
            if (type == typeof(int) || type == typeof(short) || type == typeof(byte)) return "integer";
            throw new NotSupportedException("Unsupported SQL cast type: " + type);
        }

        public virtual string Function(string operation, string[] a)
        {
            switch (operation)
            {
                case "String.Length": return "length(" + a[0] + ")";
                case "String.Position": return "instr(" + a[0] + ", " + a[1] + ")";
                case "String.IndexOf": return "(" + Function("String.Position", a) + " - 1)";
                case "String.Substring": return "substr(" + a[0] + ", (" + a[1] + " + 1)" + (a.Length == 3 ? ", " + a[2] : "") + ")";
                case "String.ToLower": return "lower(" + a[0] + ")";
                case "String.ToUpper": return "upper(" + a[0] + ")";
                case "String.Trim": return "trim(" + a[0] + ")";
                case "String.TrimStart": return "ltrim(" + a[0] + ")";
                case "String.TrimEnd": return "rtrim(" + a[0] + ")";
                case "String.Replace": return "replace(" + string.Join(", ", a) + ")";
                case "String.Concat": return "(" + string.Join(" || ", a.Select(x => "coalesce(" + x + ", '')")) + ")";
                case "Math.Abs": return "abs(" + a[0] + ")";
                case "Math.Round": return "round(" + string.Join(", ", a) + ")";
                case "Math.Floor": return "floor(" + a[0] + ")";
                case "Math.Ceiling": return "ceil(" + a[0] + ")";
                case "Math.Max": return "(case when " + a[0] + " > " + a[1] + " then " + a[0] + " else " + a[1] + " end)";
                case "Math.Min": return "(case when " + a[0] + " < " + a[1] + " then " + a[0] + " else " + a[1] + " end)";
                case "Math.Pow": return "power(" + string.Join(", ", a) + ")";
                case "Math.Sqrt": return "sqrt(" + a[0] + ")";
                case "Math.Log": return a.Length == 1 ? "ln(" + a[0] + ")" : "(ln(" + a[0] + ") / ln(" + a[1] + "))";
                case "Math.Log10": return "log10(" + a[0] + ")";
            }
            throw new NotSupportedException("Unsupported SQL function: " + operation + " for " + Kind);
        }
    }
}
