using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    // Parameter-dependent null compensation runs on a copy of the cached SQL tree.
    internal sealed class SqlNullabilityProcessor : SqlExpressionVisitor
    {
        readonly IReadOnlyList<object> _parameterValues;
        readonly SqlExpressionFactory _sqlExpressionFactory = new SqlExpressionFactory();
        internal bool CanCache { get; private set; } = true;

        internal SqlNullabilityProcessor(IReadOnlyList<object> parameterValues)
        {
            _parameterValues = parameterValues;
        }
        internal SelectExpression Process(SelectExpression query) => (SelectExpression)Visit(query);
        internal SqlExpression Process(SqlExpression expression) => (SqlExpression)Visit(expression);

        protected override Expression VisitSqlBinary(SqlBinaryExpression expression)
        {
            var visited = (SqlBinaryExpression)base.VisitSqlBinary(expression);
            if (visited.UseRelationalNulls) return visited;
            var op = visited.OperatorType;
            var left = visited.Left;
            var right = visited.Right;
            if (op == ExpressionType.Equal || op == ExpressionType.NotEqual)
            {
                var leftNull = IsNull(left);
                var rightNull = IsNull(right);
                if (leftNull || rightNull) return NullCheck(leftNull ? right : left, op == ExpressionType.NotEqual);
                var comparison = _sqlExpressionFactory.MakeBinary(op, visited.Type, left, right, useRelationalNulls: true);
                if (!left.IsNullable && !right.IsNullable) return comparison;
                var nonNullComparison = And(And(comparison, NullCheck(left, true)), NullCheck(right, true));
                if (op == ExpressionType.Equal)
                    return Or(nonNullComparison, And(NullCheck(left, false), NullCheck(right, false)));
                return Or(Or(nonNullComparison, And(NullCheck(left, false), NullCheck(right, true))),
                    And(NullCheck(left, true), NullCheck(right, false)));
            }
            if ((op == ExpressionType.GreaterThan || op == ExpressionType.GreaterThanOrEqual || op == ExpressionType.LessThan || op == ExpressionType.LessThanOrEqual)
                && visited.Type == typeof(bool) && (left.IsNullable || right.IsNullable))
            {
                var comparison = _sqlExpressionFactory.MakeBinary(op, visited.Type, left, right, useRelationalNulls: true);
                return And(And(NullCheck(left, true), NullCheck(right, true)), comparison);
            }
            return visited;
        }

        protected override Expression VisitSqlFunction(SqlFunctionExpression expression)
        {
            var visited = (SqlFunctionExpression)base.VisitSqlFunction(expression);
            if (visited.Name != "EntityIsNull" && visited.Name != "EntityIsNotNull") return visited;
            if (!IsNull(visited.Arguments[1])) throw new NotSupportedException("Entity comparisons support only null. Compare mapped key properties instead.");
            return NullCheck(visited.Arguments[0], visited.Name == "EntityIsNotNull");
        }

        bool IsNull(SqlExpression expression)
        {
            if (expression is SqlConstantExpression constant) return constant.Value == null;
            if (expression is SqlParameterExpression parameter)
            {
                if (!parameter.IsNullable) return false;
                CanCache = false;
                return _parameterValues[parameter.Index] == null;
            }
            if (expression is SqlUnaryExpression unary && unary.OperatorType == ExpressionType.Convert) return IsNull(unary.Operand);
            return false;
        }

        SqlExpression NullCheck(SqlExpression operand, bool negated)
            => _sqlExpressionFactory.MakeUnary(negated ? ExpressionType.NotEqual : ExpressionType.Equal, typeof(bool), operand);
        SqlExpression And(SqlExpression left, SqlExpression right) => _sqlExpressionFactory.MakeBinary(ExpressionType.AndAlso, typeof(bool), left, right);
        SqlExpression Or(SqlExpression left, SqlExpression right) => _sqlExpressionFactory.MakeBinary(ExpressionType.OrElse, typeof(bool), left, right);
    }
}
