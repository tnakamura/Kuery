using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class FullJoinExpression : PredicateJoinExpressionBase
    {
        internal FullJoinExpression(TableExpressionBase table, SqlExpression predicate, SqlExpression unmatchedPredicate)
            : base(table, predicate) { UnmatchedPredicate = unmatchedPredicate; }
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
