using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents a nested SELECT used as a scalar value.</summary>
    internal sealed class ScalarSubqueryExpression : SqlExpression
    {
        /// <summary>Wraps a SELECT as an expression with a scalar CLR type.</summary>
        /// <param name="subquery">The nested SELECT used by this expression.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        internal ScalarSubqueryExpression(SelectExpression subquery, Type type) : base(type, false)
        {
            Subquery = subquery;
        }
        /// <summary>Gets the SELECT used to obtain the scalar value.</summary>
        internal SelectExpression Subquery { get; }
        /// <summary>Replaces the underlying SELECT.</summary>
        /// <param name="subquery">The nested SELECT used by this expression.</param>
        /// <returns>This expression if unchanged, or the updated scalar subquery.</returns>
        internal ScalarSubqueryExpression Update(SelectExpression subquery) => ReferenceEquals(subquery, Subquery) ? this : new ScalarSubqueryExpression(subquery, Type);
        /// <summary>Visits the underlying scalar SELECT.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SelectExpression)visitor.Visit(Subquery));
    }
}
