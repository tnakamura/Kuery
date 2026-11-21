using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class CrossJoinExpression : JoinExpressionBase
    {
        internal CrossJoinExpression(TableExpressionBase table) : base(table)
        {
        }
        /// <summary>Visits the cross join table source.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var table = (TableExpressionBase)visitor.Visit(Table);
            return ReferenceEquals(table, Table) ? this : new CrossJoinExpression(table);
        }
    }
}
