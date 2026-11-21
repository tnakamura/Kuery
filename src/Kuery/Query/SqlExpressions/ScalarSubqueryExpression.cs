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
        /// <summary>Visits the underlying scalar SELECT.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SelectExpression)visitor.Visit(Subquery));
    }
}
