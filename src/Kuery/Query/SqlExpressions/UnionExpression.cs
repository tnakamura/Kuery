namespace Kuery.Query.SqlExpressions
{
    internal sealed class UnionExpression : SetOperationBase
    {
        internal UnionExpression(string alias, SelectExpression source1, SelectExpression source2, bool distinct = true)
            : base(alias, source1, source2, distinct) { }
        internal override SetOperationBase Update(SelectExpression source1, SelectExpression source2)
            => ReferenceEquals(source1, Source1) && ReferenceEquals(source2, Source2) ? this : new UnionExpression(Alias, source1, source2, IsDistinct);
    }
}
