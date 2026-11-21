using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    // The dispatch points intentionally use EF Core's names to make SQL visitors easy to compare.
    internal abstract class SqlExpressionVisitor : ExpressionVisitor
    {
        /// <summary>Dispatches SQL extension nodes to the visitor for their specific expression type.</summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The visited expression.</returns>
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

        /// <summary>Visits a SQL column reference and, by default, its child expressions.</summary>
        /// <param name="node">The SQL expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitColumn(ColumnExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL parameter reference and, by default, its child expressions.</summary>
        /// <param name="node">The SQL expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitSqlParameter(SqlParameterExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL constant value and, by default, its child expressions.</summary>
        /// <param name="node">The SQL expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitSqlConstant(SqlConstantExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL binary operation and, by default, its child expressions.</summary>
        /// <param name="node">The SQL binary expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitSqlBinary(SqlBinaryExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL unary operation and, by default, its child expressions.</summary>
        /// <param name="node">The SQL unary expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitSqlUnary(SqlUnaryExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL function call and, by default, its child expressions.</summary>
        /// <param name="node">The SQL expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitSqlFunction(SqlFunctionExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL CASE expression and, by default, its child expressions.</summary>
        /// <param name="node">The SQL expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitCase(CaseExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL membership predicate and, by default, its child expressions.</summary>
        /// <param name="node">The SQL expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitIn(InExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL LIKE predicate and, by default, its child expressions.</summary>
        /// <param name="node">The SQL expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitLike(LikeExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL EXISTS predicate and, by default, its child expressions.</summary>
        /// <param name="node">The SQL expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitExists(ExistsExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL scalar subquery and, by default, its child expressions.</summary>
        /// <param name="node">The SQL expression to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitScalarSubquery(ScalarSubqueryExpression node) => base.VisitExtension(node);
        /// <summary>Visits a SQL SELECT statement and, by default, its child expressions.</summary>
        /// <param name="node">The SELECT statement to visit.</param>
        /// <returns>The visited expression.</returns>
        protected virtual Expression VisitSelect(SelectExpression node) => base.VisitExtension(node);
    }
}
