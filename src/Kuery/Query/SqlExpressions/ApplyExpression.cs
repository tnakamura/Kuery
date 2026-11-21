using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class ApplyExpression : JoinExpressionBase
    {
        internal ApplyExpression(TableExpressionBase table, bool outer) : base(table)
        {
            IsOuter = outer;
        }

        internal bool IsOuter { get; }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var table = (TableExpressionBase)visitor.Visit(Table);
            return ReferenceEquals(table, Table) ? this : new ApplyExpression(table, IsOuter);
        }
    }
}
