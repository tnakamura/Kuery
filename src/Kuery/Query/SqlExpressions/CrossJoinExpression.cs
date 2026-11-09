using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class CrossJoinExpression : JoinExpressionBase
    {
        internal CrossJoinExpression(TableExpressionBase table) : base(table)
        {
        }
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var table = (TableExpressionBase)visitor.Visit(Table);
            return ReferenceEquals(table, Table) ? this : new CrossJoinExpression(table);
        }
    }
}
