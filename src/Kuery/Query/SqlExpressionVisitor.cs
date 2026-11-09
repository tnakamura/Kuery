using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    // The dispatch points intentionally use EF Core's names to make SQL visitors easy to compare.
    internal abstract class SqlExpressionVisitor : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression node)
        {
            if (node is ColumnExpression column) return VisitColumn(column);
            if (node is SqlParameterExpression parameter) return VisitSqlParameter(parameter);
            if (node is SqlConstantExpression constant) return VisitSqlConstant(constant);
            if (node is SqlBinaryExpression binary) return VisitSqlBinary(binary);
            if (node is SqlUnaryExpression unary) return VisitSqlUnary(unary);
            if (node is SqlFunctionExpression function) return VisitSqlFunction(function);
            if (node is CaseExpression conditional) return VisitCase(conditional);
            if (node is InExpression contains) return VisitIn(contains);
            if (node is LikeExpression like) return VisitLike(like);
            if (node is ExistsExpression exists) return VisitExists(exists);
            if (node is ScalarSubqueryExpression subquery) return VisitScalarSubquery(subquery);
            if (node is SelectExpression select) return VisitSelect(select);
            return base.VisitExtension(node);
        }

        protected virtual Expression VisitColumn(ColumnExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitSqlParameter(SqlParameterExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitSqlConstant(SqlConstantExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitSqlBinary(SqlBinaryExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitSqlUnary(SqlUnaryExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitSqlFunction(SqlFunctionExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitCase(CaseExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitIn(InExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitLike(LikeExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitExists(ExistsExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitScalarSubquery(ScalarSubqueryExpression node) => base.VisitExtension(node);
        protected virtual Expression VisitSelect(SelectExpression node) => base.VisitExtension(node);
    }
}
