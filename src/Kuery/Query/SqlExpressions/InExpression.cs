using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class InExpression : SqlExpression
    {
        internal InExpression(SqlExpression item, SqlExpression values) : base(typeof(bool), false)
        {
            Item = item;
            Values = values;
        }
        internal SqlExpression Item { get; }
        internal SqlExpression Values { get; }
        internal InExpression Update(SqlExpression item, SqlExpression values)
            => ReferenceEquals(item, Item) && ReferenceEquals(values, Values) ? this : new InExpression(item, values);
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update((SqlExpression)visitor.Visit(Item), (SqlExpression)visitor.Visit(Values));
    }
}
