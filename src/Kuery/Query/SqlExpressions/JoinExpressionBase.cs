namespace Kuery.Query.SqlExpressions
{
    /// <summary>Provides common metadata for joined table sources.</summary>
    internal abstract class JoinExpressionBase : TableExpressionBase
    {
        /// <summary>Initializes a join with its input table source.</summary>
        /// <param name="table">The input table source of the join.</param>
        protected JoinExpressionBase(TableExpressionBase table) : base(null)
        {
            Table = table;
        }
        /// <summary>Gets the table source being joined to the containing query.</summary>
        internal TableExpressionBase Table { get; }
    }
}
