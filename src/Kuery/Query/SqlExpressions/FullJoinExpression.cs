using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class FullJoinExpression : PredicateJoinExpressionBase
    {
        internal FullJoinExpression(TableExpressionBase table, SqlExpression predicate, SqlExpression unmatchedPredicate)
            : base(table, predicate) { UnmatchedPredicate = unmatchedPredicate; }
        internal SqlExpression UnmatchedPredicate { get; }
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
