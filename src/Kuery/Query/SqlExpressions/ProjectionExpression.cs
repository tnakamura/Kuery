using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class ProjectionExpression : Expression
    {
        internal ProjectionExpression(SqlExpression expression, string alias)
        {
            Expression = expression;
            Alias = alias;
        }
        internal SqlExpression Expression { get; }
        internal string Alias { get; }
        public override Type Type => Expression.Type;
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal ProjectionExpression Update(SqlExpression expression)
            => ReferenceEquals(expression, Expression) ? this : new ProjectionExpression(expression, Alias);
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SqlExpression)visitor.Visit(Expression));
    }
}
