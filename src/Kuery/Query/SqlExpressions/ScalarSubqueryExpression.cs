using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class ScalarSubqueryExpression : SqlExpression
    {
        internal ScalarSubqueryExpression(SelectExpression subquery, Type type) : base(type, false)
        {
            Subquery = subquery;
        }
        internal SelectExpression Subquery { get; }
        internal ScalarSubqueryExpression Update(SelectExpression subquery) => ReferenceEquals(subquery, Subquery) ? this : new ScalarSubqueryExpression(subquery, Type);
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SelectExpression)visitor.Visit(Subquery));
    }
}
