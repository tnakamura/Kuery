using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class LeftJoinExpression : PredicateJoinExpressionBase
    {
        internal LeftJoinExpression(TableExpressionBase table, SqlExpression predicate) : base(table, predicate)
        {
        }
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var table = (TableExpressionBase)visitor.Visit(Table);
            var predicate = (SqlExpression)visitor.Visit(JoinPredicate);
            return ReferenceEquals(table, Table) && ReferenceEquals(predicate, JoinPredicate) ? this : new LeftJoinExpression(table, predicate);
        }
    }
}
