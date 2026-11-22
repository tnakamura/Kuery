using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents a full outer join and carries the predicate used when emulating unmatched rows.</summary>
    internal sealed class FullJoinExpression : PredicateJoinExpressionBase
    {
        /// <summary>Creates a full outer join with a predicate for its emulated unmatched branch.</summary>
        /// <param name="table">The table source or mapping associated with the operation.</param>
        /// <param name="predicate">The SQL condition used to match rows in the join.</param>
        /// <param name="unmatchedPredicate">The predicate identifying unmatched rows in the emulated branch.</param>
        internal FullJoinExpression(TableExpressionBase table, SqlExpression predicate, SqlExpression unmatchedPredicate)
            : base(table, predicate) { UnmatchedPredicate = unmatchedPredicate; }
        /// <summary>Gets the condition selecting unmatched rows in the emulation of a full outer join.</summary>
        internal SqlExpression UnmatchedPredicate { get; }
        /// <summary>Visits the table, join predicate, and unmatched-row predicate.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var table = (TableExpressionBase)visitor.Visit(Table);
            var predicate = (SqlExpression)visitor.Visit(JoinPredicate);
            var unmatched = (SqlExpression)visitor.Visit(UnmatchedPredicate);
            return ReferenceEquals(table, Table) && ReferenceEquals(predicate, JoinPredicate) && ReferenceEquals(unmatched, UnmatchedPredicate)
                ? this : new FullJoinExpression(table, predicate, unmatched);
        }
    }
}
