using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    // Parameter-dependent null compensation runs on a copy of the cached SQL tree.
    /// <summary>Applies CLR null comparison semantics and identifies SQL that depends on runtime null values.</summary>
    internal sealed class SqlNullabilityProcessor : SqlExpressionVisitor
    {
        readonly IReadOnlyList<object> _parameterValues;
        readonly SqlExpressionFactory _sqlExpressionFactory = new SqlExpressionFactory();
        /// <summary>Gets whether the processed SQL remains valid for different parameter values.</summary>
        internal bool CanCache { get; private set; } = true;

        /// <summary>Creates a null semantics processor for the current parameter values.</summary>
        /// <param name="parameterValues">The runtime parameter values in slot order.</param>
        internal SqlNullabilityProcessor(IReadOnlyList<object> parameterValues)
        {
            _parameterValues = parameterValues;
        }
        /// <summary>Applies null compensation to the supplied SQL tree or value.</summary>
        /// <param name="query">The translated SELECT to process.</param>
        /// <returns>The processed SQL expression or SELECT.</returns>
        internal SelectExpression Process(SelectExpression query) => (SelectExpression)Visit(query);
        /// <summary>Applies null compensation to the supplied SQL tree or value.</summary>
        /// <param name="expression">The expression to translate, inspect, or render.</param>
        /// <returns>The processed SQL expression or SELECT.</returns>
        internal SqlExpression Process(SqlExpression expression) => (SqlExpression)Visit(expression);

        /// <summary>Rewrites nullable comparisons to preserve CLR semantics in SQL.</summary>
        /// <param name="expression">The SQL binary expression to visit.</param>
        /// <returns>The visited expression.</returns>
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

        /// <summary>Rewrites entity presence tests to SQL null checks.</summary>
        /// <param name="expression">The SQL expression to visit.</param>
        /// <returns>The visited expression.</returns>
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
