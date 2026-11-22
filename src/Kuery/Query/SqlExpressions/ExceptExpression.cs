namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents a SQL EXCEPT set operation.</summary>
    internal sealed class ExceptExpression : SetOperationBase
    {
        /// <summary>Creates an EXCEPT operation over two SELECT statements.</summary>
        /// <param name="alias">The SQL alias used to reference the table source or projected column.</param>
        /// <param name="source1">The SELECT on the left side of the set operation.</param>
        /// <param name="source2">The SELECT on the right side of the set operation.</param>
        /// <param name="distinct">Whether duplicate rows are eliminated.</param>
        internal ExceptExpression(string alias, SelectExpression source1, SelectExpression source2, bool distinct = true)
            : base(alias, source1, source2, distinct) { }
        /// <summary>Replaces the SELECT statements participating in EXCEPT.</summary>
        /// <param name="source1">The SELECT on the left side of the set operation.</param>
        /// <param name="source2">The SELECT on the right side of the set operation.</param>
        /// <returns>This operation if unchanged, or the updated EXCEPT.</returns>
        internal override SetOperationBase Update(SelectExpression source1, SelectExpression source2)
            => ReferenceEquals(source1, Source1) && ReferenceEquals(source2, Source2) ? this : new ExceptExpression(Alias, source1, source2, IsDistinct);
    }
}
