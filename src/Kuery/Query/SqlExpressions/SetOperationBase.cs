using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal abstract class SetOperationBase : TableExpressionBase
    {
        /// <summary>Initializes a set operation with two SELECT inputs.</summary>
        /// <param name="alias">The SQL alias used to reference this source.</param>
        /// <param name="source1">The SELECT statement on the left side of the set operation.</param>
        /// <param name="source2">The SELECT statement on the right side of the set operation.</param>
        /// <param name="distinct">Whether duplicate rows are removed.</param>
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
        /// <summary>Visits the two SELECT statements that participate in the set operation.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update((SelectExpression)visitor.Visit(Source1), (SelectExpression)visitor.Visit(Source2));
    }
}
