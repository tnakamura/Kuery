namespace Kuery.Query.SqlExpressions
{
    internal abstract class PredicateJoinExpressionBase : JoinExpressionBase
    {
        protected PredicateJoinExpressionBase(TableExpressionBase table, SqlExpression predicate) : base(table)
        {
            JoinPredicate = predicate;
        }
        internal SqlExpression JoinPredicate { get; }
    }
}
