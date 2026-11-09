using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class ExistsExpression : SqlExpression
    {
        internal ExistsExpression(SelectExpression subquery) : base(typeof(bool), false)
        {
            Subquery = subquery;
        }
        internal SelectExpression Subquery { get; }
        internal ExistsExpression Update(SelectExpression subquery) => ReferenceEquals(subquery, Subquery) ? this : new ExistsExpression(subquery);
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SelectExpression)visitor.Visit(Subquery));
    }
}
