using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents a left outer join with its matching condition.</summary>
    internal sealed class LeftJoinExpression : PredicateJoinExpressionBase
    {
        /// <summary>Creates a left outer join to the supplied table source.</summary>
        /// <param name="table">The table source or mapping associated with the operation.</param>
        /// <param name="predicate">The SQL condition used to match rows in the join.</param>
        internal LeftJoinExpression(TableExpressionBase table, SqlExpression predicate) : base(table, predicate)
        {
        }
        /// <summary>Visits the table and left join predicate.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var table = (TableExpressionBase)visitor.Visit(Table);
            var predicate = (SqlExpression)visitor.Visit(JoinPredicate);
            return ReferenceEquals(table, Table) && ReferenceEquals(predicate, JoinPredicate) ? this : new LeftJoinExpression(table, predicate);
        }
    }
}
