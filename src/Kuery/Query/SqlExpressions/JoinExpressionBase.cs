namespace Kuery.Query.SqlExpressions
{
    internal abstract class JoinExpressionBase : TableExpressionBase
    {
        /// <summary>Initializes a join with its input table source.</summary>
        /// <param name="table">The input table source of the join.</param>
        protected JoinExpressionBase(TableExpressionBase table) : base(null)
        {
            Table = table;
        }
        internal TableExpressionBase Table { get; }
    }
}
