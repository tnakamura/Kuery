using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Tests whether a SELECT returns any rows.</summary>
    internal sealed class ExistsExpression : SqlExpression
    {
        /// <summary>Creates an EXISTS predicate over a SELECT.</summary>
        /// <param name="subquery">The nested SELECT used by this expression.</param>
        internal ExistsExpression(SelectExpression subquery) : base(typeof(bool), false)
        {
            Subquery = subquery;
        }
        /// <summary>Gets the SELECT tested for the existence of rows.</summary>
        internal SelectExpression Subquery { get; }
        /// <summary>Replaces the SELECT being tested.</summary>
        /// <param name="subquery">The nested SELECT used by this expression.</param>
        /// <returns>This predicate if unchanged, or the updated EXISTS expression.</returns>
        internal ExistsExpression Update(SelectExpression subquery) => ReferenceEquals(subquery, Subquery) ? this : new ExistsExpression(subquery);
        /// <summary>Visits the subquery tested by EXISTS.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SelectExpression)visitor.Visit(Subquery));
    }
}
