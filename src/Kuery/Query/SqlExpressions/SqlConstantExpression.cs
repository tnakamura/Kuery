using System;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class SqlConstantExpression : SqlExpression
    {
        internal SqlConstantExpression(object value, Type type) : base(type, value == null)
        {
            Value = value;
        }
        internal object Value { get; }
    }
}
