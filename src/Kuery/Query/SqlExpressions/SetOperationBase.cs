using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal abstract class SetOperationBase : TableExpressionBase
    {
        protected SetOperationBase(string alias, SelectExpression source1, SelectExpression source2, bool distinct) : base(alias)
        {
            Source1 = source1;
            Source2 = source2;
            IsDistinct = distinct;
        }
        internal SelectExpression Source1 { get; }
        internal SelectExpression Source2 { get; }
        internal bool IsDistinct { get; }
        internal abstract SetOperationBase Update(SelectExpression source1, SelectExpression source2);
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update((SelectExpression)visitor.Visit(Source1), (SelectExpression)visitor.Visit(Source2));
    }
}
