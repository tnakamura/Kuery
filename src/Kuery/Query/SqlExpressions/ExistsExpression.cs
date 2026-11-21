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
        /// <summary>Visits the subquery tested by EXISTS.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SelectExpression)visitor.Visit(Subquery));
    }
}
