namespace Kuery.Query.SqlExpressions
{
    internal abstract class JoinExpressionBase : TableExpressionBase
    {
        protected JoinExpressionBase(TableExpressionBase table) : base(null)
        {
            Table = table;
        }
        internal TableExpressionBase Table { get; }
    }
}
