using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class OrderingExpression : Expression
    {
        internal OrderingExpression(SqlExpression expression, bool ascending)
        { Expression = expression; IsAscending = ascending; }
        internal SqlExpression Expression;
        internal bool IsAscending;
        public override Type Type => Expression.Type;
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal OrderingExpression Update(SqlExpression expression)
            => ReferenceEquals(expression, Expression) ? this : new OrderingExpression(expression, IsAscending);
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SqlExpression)visitor.Visit(Expression));
    }
}
