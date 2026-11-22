namespace Kuery.Query.SqlExpressions
{
    /// <summary>References a named database table.</summary>
    internal sealed class TableExpression : TableExpressionBase
    {
        /// <summary>Creates a table reference with a database name and SQL alias.</summary>
        /// <param name="name">The mapped database table name.</param>
        /// <param name="alias">The SQL alias used to reference the table source or projected column.</param>
        internal TableExpression(string name, string alias) : base(alias)
        {
            Name = name;
        }
        /// <summary>Gets the mapped database table name.</summary>
        internal string Name { get; }
    }
}
