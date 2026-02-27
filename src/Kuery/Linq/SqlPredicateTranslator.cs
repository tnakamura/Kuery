using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

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
                case ExpressionType.Add:
                case ExpressionType.Subtract:
                case ExpressionType.Multiply:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                    return TranslateBinary((BinaryExpression)expression, table, dialect, parameters);
                case ExpressionType.Not:
                    return "not (" + TranslateCore(((UnaryExpression)expression).Operand, table, dialect, parameters) + ")";
                case ExpressionType.Call:
                    return TranslateMethodCall((MethodCallExpression)expression, table, dialect, parameters);
                case ExpressionType.MemberAccess:
                    if (TryGetColumnExpression(expression, table, dialect, out var boolColumnSql))
                    {
                        return "(" + boolColumnSql + " = 1)";
                    }
                    goto default;
                default:
                    throw new NotSupportedException(
                        $"Unsupported predicate node: {expression.NodeType}.");
            }
        }

        private static string TranslateMethodCall(MethodCallExpression call, TableMapping table, ISqlDialect dialect, List<object> parameters)
        {
            var methodName = call.Method.Name;

            // string.Contains(value)
            if (methodName == nameof(string.Contains) && call.Object != null && call.Object.Type == typeof(string) && call.Arguments.Count == 1)
            {
                var columnSql = TranslateToColumnSql(call.Object, table, dialect, parameters);
                if (columnSql != null)
                {
                    var value = EvaluateExpression(call.Arguments[0]);
                    var paramName = AddParameter(dialect, parameters, value);
                    return BuildStringContains(columnSql, paramName, dialect);
                }
            }

            // string.StartsWith(value) / string.StartsWith(value, StringComparison)
            if (methodName == nameof(string.StartsWith) && call.Object != null && call.Arguments.Count >= 1)
            {
                var columnSql = TranslateToColumnSql(call.Object, table, dialect, parameters);
                if (columnSql != null)
                {
                    var value = EvaluateExpression(call.Arguments[0]);
                    var paramName = AddParameter(dialect, parameters, value);
                    var comparison = call.Arguments.Count == 2
                        ? (StringComparison)EvaluateExpression(call.Arguments[1])
                        : StringComparison.CurrentCulture;
                    return BuildStartsWith(columnSql, paramName, value?.ToString()?.Length ?? 0, comparison, dialect);
                }
            }

            // string.EndsWith(value) / string.EndsWith(value, StringComparison)
            if (methodName == nameof(string.EndsWith) && call.Object != null && call.Arguments.Count >= 1)
            {
                var columnSql = TranslateToColumnSql(call.Object, table, dialect, parameters);
                if (columnSql != null)
                {
                    var value = EvaluateExpression(call.Arguments[0]);
                    var paramName = AddParameter(dialect, parameters, value);
                    var comparison = call.Arguments.Count == 2
                        ? (StringComparison)EvaluateExpression(call.Arguments[1])
                        : StringComparison.CurrentCulture;
                    return BuildEndsWith(columnSql, paramName, value?.ToString()?.Length ?? 0, comparison, dialect);
                }
            }

            // string.Replace(oldValue, newValue)
            if (methodName == nameof(string.Replace) && call.Object != null && call.Arguments.Count == 2)
            {
                var columnSql = TranslateToColumnSql(call.Object, table, dialect, parameters);
                if (columnSql != null)
                {
                    var oldValue = EvaluateExpression(call.Arguments[0]);
                    var newValue = EvaluateExpression(call.Arguments[1]);
                    var oldParam = AddParameter(dialect, parameters, oldValue);
                    var newParam = AddParameter(dialect, parameters, newValue);
                    return "replace(" + columnSql + ", " + oldParam + ", " + newParam + ")";
                }
            }

            // Enumerable.Contains(source, value) or span-based Contains - static 2-arg form
            if (methodName == nameof(Enumerable.Contains) && call.Object == null && call.Arguments.Count == 2)
            {
                if (TryGetColumnExpression(call.Arguments[1], table, dialect, out var columnSql))
                {
                    var collection = EvaluateCollectionExpression(call.Arguments[0]);
                    return BuildInClause(columnSql, (IEnumerable)collection, dialect, parameters);
                }
            }

            // collection.Contains(value) - instance 1-arg form
            if (methodName == nameof(Enumerable.Contains) && call.Object != null && call.Arguments.Count == 1)
            {
                // string.Contains is handled above; this handles IEnumerable<T>.Contains
                if (call.Object.Type != typeof(string) && TryGetColumnExpression(call.Arguments[0], table, dialect, out var columnSql))
                {
                    var collection = EvaluateExpression(call.Object);
                    return BuildInClause(columnSql, (IEnumerable)collection, dialect, parameters);
                }
            }

            // object.Equals(value) - instance 1-arg form
            if (methodName == nameof(object.Equals) && call.Object != null && call.Arguments.Count == 1)
            {
                if (TryGetColumnExpression(call.Object, table, dialect, out var columnSql))
                {
                    var value = EvaluateExpression(call.Arguments[0]);
                    return BuildColumnValueComparison(columnSql, ExpressionType.Equal, value, dialect, parameters);
                }
                var transformed = TranslateToColumnSql(call.Object, table, dialect, parameters);
                if (transformed != null)
                {
                    var value = EvaluateExpression(call.Arguments[0]);
                    return BuildColumnValueComparison(transformed, ExpressionType.Equal, value, dialect, parameters);
                }
            }

            // string.IsNullOrEmpty(value) - static 1-arg form
            if (methodName == nameof(string.IsNullOrEmpty) && call.Object == null && call.Arguments.Count == 1
                && call.Method.DeclaringType == typeof(string))
            {
                if (TryGetColumnExpression(call.Arguments[0], table, dialect, out var columnSql))
                {
                    return "(" + columnSql + " is null or " + columnSql + " = '')";
                }
                var transformed2 = TranslateToColumnSql(call.Arguments[0], table, dialect, parameters);
                if (transformed2 != null)
                {
                    return "(" + transformed2 + " is null or " + transformed2 + " = '')";
                }
            }

            throw new NotSupportedException($"Unsupported method call: {call.Method.DeclaringType?.Name}.{methodName}.");
        }

        private static string TranslateToColumnSql(Expression expression, TableMapping table, ISqlDialect dialect)
        {
            return TranslateToColumnSql(expression, table, dialect, null);
        }

        private static string TranslateToColumnSql(Expression expression, TableMapping table, ISqlDialect dialect, List<object> parameters)
        {
            // Handle x.Prop.ToLower() / x.Prop.ToUpper() / x.Prop.Replace() wrapping
            if (expression is MethodCallExpression innerCall && innerCall.Object != null)
            {
                if (innerCall.Method.Name == nameof(string.ToLower) && innerCall.Arguments.Count == 0)
                {
                    var inner = TranslateToColumnSql(innerCall.Object, table, dialect, parameters);
                    if (inner != null) return "lower(" + inner + ")";
                }
                if (innerCall.Method.Name == nameof(string.ToUpper) && innerCall.Arguments.Count == 0)
                {
                    var inner = TranslateToColumnSql(innerCall.Object, table, dialect, parameters);
                    if (inner != null) return "upper(" + inner + ")";
                }
                if (innerCall.Method.Name == nameof(string.Replace) && innerCall.Arguments.Count == 2)
                {
                    var inner = TranslateToColumnSql(innerCall.Object, table, dialect, parameters);
                    if (inner != null)
                    {
                        var oldVal = EvaluateExpression(innerCall.Arguments[0]);
                        var newVal = EvaluateExpression(innerCall.Arguments[1]);
                        if (parameters != null)
                        {
                            var oldParam = AddParameter(dialect, parameters, oldVal);
                            var newParam = AddParameter(dialect, parameters, newVal);
                            return "replace(" + inner + ", " + oldParam + ", " + newParam + ")";
                        }
                        return "replace(" + inner + ", '" + oldVal?.ToString()?.Replace("'", "''") + "', '" + newVal?.ToString()?.Replace("'", "''") + "')";
                    }
                }
            }

            if (TryGetColumnExpression(expression, table, dialect, out var columnSql))
            {
                return columnSql;
            }

            return null;
        }

        private static string BuildStringContains(string columnSql, string paramName, ISqlDialect dialect)
        {
            if (dialect.Kind == SqlDialectKind.SqlServer)
            {
                return "(CHARINDEX(" + paramName + ", " + columnSql + ") > 0)";
            }
            if (dialect.Kind == SqlDialectKind.PostgreSql)
            {
                return "(strpos(" + columnSql + ", " + paramName + ") > 0)";
            }
            return "(instr(" + columnSql + ", " + paramName + ") > 0)";
        }

        private static string BuildStartsWith(string columnSql, string paramName, int valueLength, StringComparison comparison, ISqlDialect dialect)
        {
            switch (comparison)
            {
                case StringComparison.Ordinal:
                case StringComparison.CurrentCulture:
                    if (dialect.Kind == SqlDialectKind.SqlServer)
                    {
                        return "(SUBSTRING(" + columnSql + ", 1, " + valueLength + ") = " + paramName + ")";
                    }
                    return "(substr(" + columnSql + ", 1, " + valueLength + ") = " + paramName + ")";
                case StringComparison.OrdinalIgnoreCase:
                case StringComparison.CurrentCultureIgnoreCase:
                    if (dialect.Kind == SqlDialectKind.SqlServer)
                    {
                        return "(" + columnSql + " like (" + paramName + " + N'%'))";
                    }
                    return "(" + columnSql + " like (" + paramName + " || '%'))";
                default:
                    throw new NotSupportedException($"Unsupported StringComparison: {comparison}");
            }
        }

        private static string BuildEndsWith(string columnSql, string paramName, int valueLength, StringComparison comparison, ISqlDialect dialect)
        {
            switch (comparison)
            {
                case StringComparison.Ordinal:
                case StringComparison.CurrentCulture:
                    if (dialect.Kind == SqlDialectKind.SqlServer)
                    {
                        return "(SUBSTRING(" + columnSql + ", LEN(" + columnSql + ") - " + valueLength + " + 1, " + valueLength + ") = " + paramName + ")";
                    }
                    return "(substr(" + columnSql + ", length(" + columnSql + ") - " + valueLength + " + 1, " + valueLength + ") = " + paramName + ")";
                case StringComparison.OrdinalIgnoreCase:
                case StringComparison.CurrentCultureIgnoreCase:
                    if (dialect.Kind == SqlDialectKind.SqlServer)
                    {
                        return "(" + columnSql + " like (N'%' + " + paramName + "))";
                    }
                    return "(" + columnSql + " like ('%' || " + paramName + "))";
                default:
                    throw new NotSupportedException($"Unsupported StringComparison: {comparison}");
            }
        }

        private static string BuildInClause(string columnSql, IEnumerable collection, ISqlDialect dialect, List<object> parameters)
        {
            var sb = new StringBuilder();
            sb.Append("(");
            sb.Append(columnSql);
            sb.Append(" in (");
            var first = true;
            foreach (var item in collection)
            {
                if (!first) sb.Append(", ");
                sb.Append(AddParameter(dialect, parameters, item));
                first = false;
            }
            sb.Append("))");
            return sb.ToString();
        }

        private static string AddParameter(ISqlDialect dialect, List<object> parameters, object value)
        {
            var paramBaseName = "p" + (parameters.Count + 1).ToString();
            var parameterName = dialect.FormatParameterName(paramBaseName);
            parameters.Add(value);
            return parameterName;
        }

        private static bool IsArithmeticOperator(ExpressionType nodeType)
        {
            switch (nodeType)
            {
                case ExpressionType.Add:
                case ExpressionType.Subtract:
                case ExpressionType.Multiply:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                    return true;
                default:
                    return false;
            }
        }

        private static string TryTranslateArithmeticSql(Expression expression, TableMapping table, ISqlDialect dialect, List<object> parameters)
        {
            if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                return TryTranslateArithmeticSql(unary.Operand, table, dialect, parameters);
            }

            if (TryGetColumnExpression(expression, table, dialect, out var columnSql))
            {
                return columnSql;
            }

            var transformed = TranslateToColumnSql(expression, table, dialect, parameters);
            if (transformed != null)
            {
                return transformed;
            }

            if (expression is BinaryExpression binary && IsArithmeticOperator(binary.NodeType))
            {
                var left = TryTranslateArithmeticSql(binary.Left, table, dialect, parameters);
                var right = TryTranslateArithmeticSql(binary.Right, table, dialect, parameters);
                if (left != null && right != null)
                {
                    return "(" + left + " " + ToSqlOperator(binary.NodeType) + " " + right + ")";
                }
            }

            if (expression is ConstantExpression || expression is MemberExpression)
            {
                var value = EvaluateExpression(expression);
                return AddParameter(dialect, parameters, value);
            }

            return null;
        }

        private static string TranslateBinary(BinaryExpression expression, TableMapping table, ISqlDialect dialect, List<object> parameters)
        {
            // Handle arithmetic sub-expressions (e.g. x.Price * x.Qty > 100)
            if (IsArithmeticOperator(expression.NodeType))
            {
                var arithLeft = TryTranslateArithmeticSql(expression.Left, table, dialect, parameters);
                var arithRight = TryTranslateArithmeticSql(expression.Right, table, dialect, parameters);
                if (arithLeft != null && arithRight != null)
                {
                    return "(" + arithLeft + " " + ToSqlOperator(expression.NodeType) + " " + arithRight + ")";
                }
            }

            var leftIsColumn = TryGetColumnExpression(expression.Left, table, dialect, out var leftColumn);
            var rightIsColumn = TryGetColumnExpression(expression.Right, table, dialect, out var rightColumn);

            // Also try column-transforming expressions (Replace, ToLower, ToUpper)
            // and arithmetic sub-expressions
            if (!leftIsColumn)
            {
                var leftTransformed = TranslateToColumnSql(expression.Left, table, dialect, parameters);
                if (leftTransformed != null)
                {
                    leftColumn = leftTransformed;
                    leftIsColumn = true;
                }
                else if (expression.Left is BinaryExpression leftBin && IsArithmeticOperator(leftBin.NodeType))
                {
                    var arith = TryTranslateArithmeticSql(expression.Left, table, dialect, parameters);
                    if (arith != null)
                    {
                        leftColumn = arith;
                        leftIsColumn = true;
                    }
                }
            }
            if (!rightIsColumn)
            {
                var rightTransformed = TranslateToColumnSql(expression.Right, table, dialect, parameters);
                if (rightTransformed != null)
                {
                    rightColumn = rightTransformed;
                    rightIsColumn = true;
                }
                else if (expression.Right is BinaryExpression rightBin && IsArithmeticOperator(rightBin.NodeType))
                {
                    var arith = TryTranslateArithmeticSql(expression.Right, table, dialect, parameters);
                    if (arith != null)
                    {
                        rightColumn = arith;
                        rightIsColumn = true;
                    }
                }
            }

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

            var parameterName = AddParameter(dialect, parameters, value);
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

        private static object EvaluateCollectionExpression(Expression expression)
        {
            // In .NET 10+, the C# compiler may resolve array.Contains() to a
            // ReadOnlySpan<T>-based overload, wrapping the array in a method call
            // that returns ReadOnlySpan<T>. Expression trees cannot compile ref structs,
            // so we unwrap the conversion to get the underlying array/collection.
            if (expression is MethodCallExpression methodCall)
            {
                if (methodCall.Arguments.Count > 0)
                {
                    return EvaluateCollectionExpression(methodCall.Arguments[0]);
                }
                if (methodCall.Object != null)
                {
                    return EvaluateCollectionExpression(methodCall.Object);
                }
            }

            return EvaluateExpression(expression);
        }

        private static object EvaluateExpression(Expression expression)
        {
            switch (expression)
            {
                case ConstantExpression constant:
                    return constant.Value;
                case MemberExpression member when member.Expression != null:
                    var obj = EvaluateExpression(member.Expression);
                    if (member.Member is System.Reflection.FieldInfo field)
                        return field.GetValue(obj);
                    if (member.Member is System.Reflection.PropertyInfo prop)
                        return prop.GetValue(obj);
                    break;
                case UnaryExpression unary when unary.NodeType == ExpressionType.Convert:
                    return EvaluateExpression(unary.Operand);
                case NewArrayExpression newArray:
                    var elementType = newArray.Type.GetElementType();
                    var array = Array.CreateInstance(elementType, newArray.Expressions.Count);
                    for (var i = 0; i < newArray.Expressions.Count; i++)
                    {
                        array.SetValue(EvaluateExpression(newArray.Expressions[i]), i);
                    }
                    return array;
            }

            // Generic fallback: compile with interpretation if available
            var lambda = Expression.Lambda(expression);
            var compileWithInterp = typeof(LambdaExpression).GetMethod(
                "Compile", new[] { typeof(bool) });
            Delegate compiled;
            if (compileWithInterp != null)
            {
                compiled = (Delegate)compileWithInterp.Invoke(
                    lambda, new object[] { true });
            }
            else
            {
                compiled = lambda.Compile();
            }
            var invokeMethod = compiled.GetType().GetMethod("Invoke");
            return invokeMethod.Invoke(compiled, null);
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
                case ExpressionType.Add:
                    return "+";
                case ExpressionType.Subtract:
                    return "-";
                case ExpressionType.Multiply:
                    return "*";
                case ExpressionType.Divide:
                    return "/";
                case ExpressionType.Modulo:
                    return "%";
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
