using System;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class ColumnExpression : SqlExpression
    {
        internal ColumnExpression(string alias, string name, Type type, bool nullable)
            : base(type, nullable) { TableAlias = alias; Name = name; }
        internal string TableAlias { get; }
        internal string Name { get; }
    }
}
