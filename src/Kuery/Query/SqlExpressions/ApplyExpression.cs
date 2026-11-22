using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents a lateral join to a table source that can reference the outer row.</summary>
    internal sealed class ApplyExpression : JoinExpressionBase
    {
        /// <summary>Creates an inner or outer lateral join.</summary>
        /// <param name="table">The table source or mapping associated with the operation.</param>
        /// <param name="outer">Whether to preserve the outer row when the lateral source is empty.</param>
        internal ApplyExpression(TableExpressionBase table, bool outer) : base(table)
        {
            IsOuter = outer;
        }

        /// <summary>Gets whether the join preserves outer rows when the lateral source is empty.</summary>
        internal bool IsOuter { get; }

        /// <summary>Visits the correlated table source.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var table = (TableExpressionBase)visitor.Visit(Table);
            return ReferenceEquals(table, Table) ? this : new ApplyExpression(table, IsOuter);
        }
    }
}
