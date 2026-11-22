namespace Kuery.Query.SqlExpressions
{
    /// <summary>Provides common metadata for joins with an ON condition.</summary>
    internal abstract class PredicateJoinExpressionBase : JoinExpressionBase
    {
        /// <summary>Initializes a join with its input table source and join condition.</summary>
        /// <param name="table">The input table source of the join.</param>
        /// <param name="predicate">The SQL condition used to match rows.</param>
        protected PredicateJoinExpressionBase(TableExpressionBase table, SqlExpression predicate) : base(table)
        {
            JoinPredicate = predicate;
        }
        /// <summary>Gets the ON condition used to match rows.</summary>
        internal SqlExpression JoinPredicate { get; }
    }
}
