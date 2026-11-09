using System;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class SqlParameterExpression : SqlExpression
    {
        internal SqlParameterExpression(int index, Type type)
            : base(type, !type.IsValueType || Nullable.GetUnderlyingType(type) != null) { Index = index; }
        internal int Index { get; }
    }
}
