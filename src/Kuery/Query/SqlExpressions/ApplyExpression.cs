using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class ApplyExpression : JoinExpressionBase
    {
        internal ApplyExpression(TableExpressionBase table, bool outer) : base(table)
        {
            IsOuter = outer;
        }

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
