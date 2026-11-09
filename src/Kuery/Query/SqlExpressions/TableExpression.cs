namespace Kuery.Query.SqlExpressions
{
    internal sealed class TableExpression : TableExpressionBase
    {
        internal TableExpression(string name, string alias) : base(alias)
        {
            Name = name;
        }
        internal string Name { get; }
    }
}
