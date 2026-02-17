using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Kuery.Linq
{
    internal sealed class SqlPredicateTranslator
    {
        internal string Translate(Expression expression, TableMapping table, ISqlDialect dialect, List<object> parameters)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));
            if (table == null) throw new ArgumentNullException(nameof(table));
            if (dialect == null) throw new ArgumentNullException(nameof(dialect));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            return TranslateCore(expression, table, dialect, parameters);
        }

        private static string TranslateCore(Expression expression, TableMapping table, ISqlDialect dialect, List<object> parameters)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.AndAlso:
                case ExpressionType.OrElse:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                    return TranslateBinary((BinaryExpression)expression, table, dialect, parameters);
                case ExpressionType.Not:
                    return "not (" + TranslateCore(((UnaryExpression)expression).Operand, table, dialect, parameters) + ")";
                default:
                    throw new NotSupportedException(
                        $"Unsupported predicate node: {expression.NodeType}. Supported nodes: AndAlso, OrElse, Equal, NotEqual, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Not.");
            }
        }

        private static string TranslateBinary(BinaryExpression expression, TableMapping table, ISqlDialect dialect, List<object> parameters)
        {
            var leftIsColumn = TryGetColumnExpression(expression.Left, table, dialect, out var leftColumn);
            var rightIsColumn = TryGetColumnExpression(expression.Right, table, dialect, out var rightColumn);

            if (leftIsColumn && !rightIsColumn)
            {
                return BuildColumnValueComparison(leftColumn, expression.NodeType, EvaluateExpression(expression.Right), dialect, parameters);
            }

            if (!leftIsColumn && rightIsColumn)
            {
                var swapped = SwapBinaryType(expression.NodeType);
                return BuildColumnValueComparison(rightColumn, swapped, EvaluateExpression(expression.Left), dialect, parameters);
            }

            if (leftIsColumn && rightIsColumn)
            {
                var op = ToSqlOperator(expression.NodeType);
                return "(" + leftColumn + " " + op + " " + rightColumn + ")";
            }

            var left = TranslateCore(expression.Left, table, dialect, parameters);
            var right = TranslateCore(expression.Right, table, dialect, parameters);
            var expressionType = expression.NodeType;
            if (expressionType == ExpressionType.AndAlso || expressionType == ExpressionType.OrElse)
            {
                return "(" + left + " " + ToSqlOperator(expressionType) + " " + right + ")";
            }

            throw new NotSupportedException($"Unsupported binary expression: {expression}. At least one side must resolve to a mapped property.");
        }

        private static string BuildColumnValueComparison(string columnSql, ExpressionType nodeType, object value, ISqlDialect dialect, List<object> parameters)
        {
            if (value == null)
            {
                if (nodeType == ExpressionType.Equal)
                {
                    return "(" + columnSql + " is null)";
                }

                if (nodeType == ExpressionType.NotEqual)
                {
                    return "(" + columnSql + " is not null)";
                }

                throw new NotSupportedException($"Null comparison is not supported for operator: {nodeType}");
            }

            var paramBaseName = "p" + (parameters.Count + 1).ToString();
            var parameterName = dialect.FormatParameterName(paramBaseName);
            parameters.Add(value);
            return "(" + columnSql + " " + ToSqlOperator(nodeType) + " " + parameterName + ")";
        }

        private static bool TryGetColumnExpression(Expression expression, TableMapping table, ISqlDialect dialect, out string columnSql)
        {
            if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                expression = unary.Operand;
            }

            if (expression is MemberExpression member && member.Expression?.NodeType == ExpressionType.Parameter)
            {
                var column = table.FindColumnWithPropertyName(member.Member.Name);
                if (column != null)
                {
                    columnSql = dialect.EscapeIdentifier(column.Name);
                    return true;
                }
            }

            columnSql = null;
            return false;
        }

        private static object EvaluateExpression(Expression expression)
        {
            var lambda = Expression.Lambda(expression);
            return lambda.Compile().DynamicInvoke();
        }

        private static string ToSqlOperator(ExpressionType expressionType)
        {
            switch (expressionType)
            {
                case ExpressionType.AndAlso:
                    return "and";
                case ExpressionType.OrElse:
                    return "or";
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "!=";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                default:
                    throw new NotSupportedException($"Unsupported expression type: {expressionType}");
            }
        }

        private static ExpressionType SwapBinaryType(ExpressionType expressionType)
        {
            switch (expressionType)
            {
                case ExpressionType.GreaterThan:
                    return ExpressionType.LessThan;
                case ExpressionType.GreaterThanOrEqual:
                    return ExpressionType.LessThanOrEqual;
                case ExpressionType.LessThan:
                    return ExpressionType.GreaterThan;
                case ExpressionType.LessThanOrEqual:
                    return ExpressionType.GreaterThanOrEqual;
                default:
                    return expressionType;
            }
        }
    }
}
