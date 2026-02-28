using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Kuery.Linq
{
    internal sealed class SelectSqlGenerator
    {
        readonly SqlPredicateTranslator _predicateTranslator = new SqlPredicateTranslator();

        internal GeneratedSql GenerateSetOperation(SetOperationQueryModel model, ISqlDialect dialect)
        {
            if (model == null) throw new ArgumentNullException(nameof(model));
            if (dialect == null) throw new ArgumentNullException(nameof(dialect));

            var parameters = new List<object>();
            var sql = new StringBuilder();
            AppendSetOperand(sql, model.Left, dialect, parameters);
            sql.Append(GetSetOperator(model.Operation));
            AppendSetOperand(sql, model.Right, dialect, parameters);
            return new GeneratedSql(sql.ToString(), parameters);
        }

        private void AppendSetOperand(StringBuilder sql, object operand, ISqlDialect dialect, List<object> parameters)
        {
            if (operand is SelectQueryModel selectModel)
            {
                sql.Append(GenerateCore(selectModel, dialect, parameters));
            }
            else if (operand is SetOperationQueryModel setModel)
            {
                AppendSetOperand(sql, setModel.Left, dialect, parameters);
                sql.Append(GetSetOperator(setModel.Operation));
                AppendSetOperand(sql, setModel.Right, dialect, parameters);
            }
            else
            {
                throw new NotSupportedException($"Unsupported set operation operand type: {operand?.GetType()}");
            }
        }

        private static string GetSetOperator(SetOperationKind kind)
        {
            switch (kind)
            {
                case SetOperationKind.Union: return " union ";
                case SetOperationKind.UnionAll: return " union all ";
                case SetOperationKind.Intersect: return " intersect ";
                case SetOperationKind.Except: return " except ";
                default: throw new NotSupportedException($"Unsupported set operation: {kind}");
            }
        }

        internal GeneratedSql Generate(SelectQueryModel model, ISqlDialect dialect)
        {
            var parameters = new List<object>();
            var sql = GenerateCore(model, dialect, parameters);
            return new GeneratedSql(sql, parameters);
        }

        private string GenerateCore(SelectQueryModel model, ISqlDialect dialect, List<object> parameters)
        {
            if (model == null) throw new ArgumentNullException(nameof(model));
            if (dialect == null) throw new ArgumentNullException(nameof(dialect));

            var sql = new StringBuilder();

            var effectiveTake = model.Take;
            if ((model.Terminal == QueryTerminalKind.First || model.Terminal == QueryTerminalKind.FirstOrDefault
                || model.Terminal == QueryTerminalKind.Last || model.Terminal == QueryTerminalKind.LastOrDefault
                || model.Terminal == QueryTerminalKind.ElementAt || model.Terminal == QueryTerminalKind.ElementAtOrDefault
                || model.Terminal == QueryTerminalKind.Single || model.Terminal == QueryTerminalKind.SingleOrDefault) && !effectiveTake.HasValue)
            {
                effectiveTake = model.Terminal == QueryTerminalKind.Single || model.Terminal == QueryTerminalKind.SingleOrDefault ? 2 : 1;
            }

            sql.Append("select ");
            if (model.Terminal == QueryTerminalKind.Count || model.Terminal == QueryTerminalKind.LongCount
                || model.Terminal == QueryTerminalKind.Any || model.Terminal == QueryTerminalKind.All)
            {
                sql.Append("count(*)");
            }
            else if (model.Terminal == QueryTerminalKind.Sum || model.Terminal == QueryTerminalKind.Min
                || model.Terminal == QueryTerminalKind.Max || model.Terminal == QueryTerminalKind.Average)
            {
                var funcName = model.Terminal == QueryTerminalKind.Average ? "avg" : model.Terminal.ToString().ToLower();
                var columnExpr = GetAggregateColumn(model, dialect);
                sql.Append(funcName);
                sql.Append("(");
                sql.Append(columnExpr);
                sql.Append(")");
            }
            else
            {
                if (dialect.Kind == SqlDialectKind.SqlServer && effectiveTake.HasValue && !model.Skip.HasValue)
                {
                    sql.Append("TOP (");
                    sql.Append(effectiveTake.Value);
                    sql.Append(") ");
                }

                if (model.IsDistinct)
                {
                    sql.Append("distinct ");
                }

                if (model.GroupBySelectItems != null && model.GroupBySelectItems.Count > 0)
                {
                    for (var i = 0; i < model.GroupBySelectItems.Count; i++)
                    {
                        if (i > 0)
                        {
                            sql.Append(", ");
                        }
                        var item = model.GroupBySelectItems[i];
                        if (item.IsKey)
                        {
                            sql.Append(dialect.EscapeIdentifier(item.SourceColumn.Name));
                        }
                        else
                        {
                            sql.Append(item.AggregateFunction);
                            sql.Append("(");
                            sql.Append(item.SourceColumn != null ? dialect.EscapeIdentifier(item.SourceColumn.Name) : "*");
                            sql.Append(")");
                        }
                    }
                }
                else if (model.ProjectedColumns != null && model.ProjectedColumns.Count > 0)
                {
                    for (var i = 0; i < model.ProjectedColumns.Count; i++)
                    {
                        if (i > 0)
                        {
                            sql.Append(", ");
                        }
                        sql.Append(dialect.EscapeIdentifier(model.ProjectedColumns[i].SourceColumn.Name));
                    }
                }
                else if (model.Joins != null && model.Joins.Count > 0)
                {
                    AppendJoinColumns(sql, model.Table, model.Joins, dialect);
                }
                else
                {
                    sql.Append("*");
                }
            }

            sql.Append(" from ");
            sql.Append(dialect.EscapeIdentifier(model.Table.TableName));

            if (model.Joins != null && model.Joins.Count > 0)
            {
                foreach (var join in model.Joins)
                {
                    sql.Append(join.IsLeftJoin ? " left join " : " inner join ");
                    sql.Append(dialect.EscapeIdentifier(join.InnerTable.TableName));
                    sql.Append(" on ");
                    for (var ki = 0; ki < join.KeyPairs.Count; ki++)
                    {
                        if (ki > 0) sql.Append(" and ");
                        var kp = join.KeyPairs[ki];
                        sql.Append(dialect.EscapeIdentifier(kp.OuterKeyTable.TableName));
                        sql.Append(".");
                        sql.Append(dialect.EscapeIdentifier(kp.OuterKeyColumn.Name));
                        sql.Append(" = ");
                        sql.Append(dialect.EscapeIdentifier(join.InnerTable.TableName));
                        sql.Append(".");
                        sql.Append(dialect.EscapeIdentifier(kp.InnerKeyColumn.Name));
                    }
                }
            }

            if (model.Terminal == QueryTerminalKind.All && model.AllPredicate != null)
            {
                var negated = Expression.Not(model.AllPredicate);
                var allWhere = model.Predicate != null
                    ? Expression.AndAlso(model.Predicate, negated)
                    : (Expression)negated;
                sql.Append(" where ");
                var predicateSql = _predicateTranslator.Translate(allWhere, model.Table, dialect, parameters);
                sql.Append(model.Joins != null && model.Joins.Count > 0 ? QualifyColumns(predicateSql, model.Table, dialect) : predicateSql);
            }
            else if (model.Predicate != null)
            {
                sql.Append(" where ");
                var predicateSql = _predicateTranslator.Translate(model.Predicate, model.Table, dialect, parameters);
                sql.Append(model.Joins != null && model.Joins.Count > 0 ? QualifyColumns(predicateSql, model.Table, dialect) : predicateSql);
            }

            if (model.GroupByColumns != null && model.GroupByColumns.Count > 0)
            {
                sql.Append(" group by ");
                for (var i = 0; i < model.GroupByColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        sql.Append(", ");
                    }
                    sql.Append(dialect.EscapeIdentifier(model.GroupByColumns[i].Name));
                }
            }

            if (model.HavingPredicate != null)
            {
                sql.Append(" having ");
                var havingSql = TranslateHavingExpression(model.HavingPredicate, model.HavingGroupParameter, model, dialect, parameters);
                sql.Append(havingSql);
            }

            AppendOrderBy(sql, model, dialect);
            AppendPaging(sql, model, effectiveTake, dialect);

            return sql.ToString();
        }

        private static void AppendOrderBy(StringBuilder sql, SelectQueryModel model, ISqlDialect dialect)
        {
            var isLast = model.Terminal == QueryTerminalKind.Last || model.Terminal == QueryTerminalKind.LastOrDefault;

            if (model.Orderings.Count == 0)
            {
                if ((model.Skip.HasValue || isLast) && model.Table.PK != null)
                {
                    sql.Append(" order by ");
                    sql.Append(dialect.EscapeIdentifier(model.Table.PK.Name));
                    if (isLast)
                    {
                        sql.Append(" desc");
                    }
                }
                return;
            }

            sql.Append(" order by ");
            for (var i = 0; i < model.Orderings.Count; i++)
            {
                if (i > 0)
                {
                    sql.Append(", ");
                }

                var ordering = model.Orderings[i];
                sql.Append(dialect.EscapeIdentifier(ordering.Column.Name));
                // For Last/LastOrDefault, reverse the sort direction
                var ascending = isLast ? !ordering.Ascending : ordering.Ascending;
                if (!ascending)
                {
                    sql.Append(" desc");
                }
            }
        }

        private static void AppendPaging(StringBuilder sql, SelectQueryModel model, int? effectiveTake, ISqlDialect dialect)
        {
            if (dialect.Kind == SqlDialectKind.SqlServer)
            {
                if (model.Skip.HasValue)
                {
                    sql.Append(" OFFSET ");
                    sql.Append(model.Skip.Value);
                    sql.Append(" ROWS");
                    if (effectiveTake.HasValue)
                    {
                        sql.Append(" FETCH NEXT ");
                        sql.Append(effectiveTake.Value);
                        sql.Append(" ROWS ONLY");
                    }
                }

                return;
            }

            if (effectiveTake.HasValue)
            {
                sql.Append(" limit ");
                sql.Append(effectiveTake.Value);
            }

            if (model.Skip.HasValue)
            {
                if (!effectiveTake.HasValue)
                {
                    if (dialect.Kind == SqlDialectKind.Sqlite)
                    {
                        sql.Append(" limit -1");
                    }
                    else
                    {
                        sql.Append(" limit 9223372036854775807");
                    }
                }

                sql.Append(" offset ");
                sql.Append(model.Skip.Value);
            }
        }

        private static string GetAggregateColumn(SelectQueryModel model, ISqlDialect dialect)
        {
            if (model.AggregateSelector != null)
            {
                var body = model.AggregateSelector.Body;
                if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
                {
                    body = unary.Operand;
                }

                if (body is MemberExpression member && member.Expression?.NodeType == ExpressionType.Parameter)
                {
                    var col = model.Table.FindColumnWithPropertyName(member.Member.Name);
                    if (col != null)
                    {
                        return dialect.EscapeIdentifier(col.Name);
                    }
                }

                throw new NotSupportedException($"Unsupported aggregate selector: {model.AggregateSelector}. Only direct member access is supported.");
            }

            if (model.ProjectedColumns != null && model.ProjectedColumns.Count == 1)
            {
                return dialect.EscapeIdentifier(model.ProjectedColumns[0].SourceColumn.Name);
            }

            return "*";
        }

        private static void AppendJoinColumns(StringBuilder sql, TableMapping outerTable, IReadOnlyList<JoinClause> joins, ISqlDialect dialect)
        {
            var first = true;
            foreach (var col in outerTable.Columns)
            {
                if (!first) sql.Append(", ");
                sql.Append(dialect.EscapeIdentifier(outerTable.TableName));
                sql.Append(".");
                sql.Append(dialect.EscapeIdentifier(col.Name));
                first = false;
            }
            foreach (var join in joins)
            {
                foreach (var col in join.InnerTable.Columns)
                {
                    sql.Append(", ");
                    sql.Append(dialect.EscapeIdentifier(join.InnerTable.TableName));
                    sql.Append(".");
                    sql.Append(dialect.EscapeIdentifier(col.Name));
                }
            }
        }

        private static string QualifyColumns(string predicateSql, TableMapping table, ISqlDialect dialect)
        {
            var tablePrefix = dialect.EscapeIdentifier(table.TableName) + ".";
            foreach (var col in table.Columns)
            {
                var escaped = dialect.EscapeIdentifier(col.Name);
                predicateSql = predicateSql.Replace(escaped, tablePrefix + escaped);
            }
            return predicateSql;
        }

        private static string TranslateHavingExpression(
            Expression expression,
            ParameterExpression groupParam,
            SelectQueryModel model,
            ISqlDialect dialect,
            List<object> parameters)
        {
            if (expression is BinaryExpression binary)
            {
                if (binary.NodeType == ExpressionType.AndAlso || binary.NodeType == ExpressionType.OrElse)
                {
                    var left = TranslateHavingExpression(binary.Left, groupParam, model, dialect, parameters);
                    var right = TranslateHavingExpression(binary.Right, groupParam, model, dialect, parameters);
                    var op = binary.NodeType == ExpressionType.AndAlso ? " and " : " or ";
                    return "(" + left + op + right + ")";
                }

                var leftSql = TranslateHavingOperand(binary.Left, groupParam, model, dialect, parameters);
                var rightSql = TranslateHavingOperand(binary.Right, groupParam, model, dialect, parameters);
                var sqlOp = ToSqlOperator(binary.NodeType);
                return "(" + leftSql + " " + sqlOp + " " + rightSql + ")";
            }

            if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Not)
            {
                return "not (" + TranslateHavingExpression(unary.Operand, groupParam, model, dialect, parameters) + ")";
            }

            throw new NotSupportedException($"Unsupported HAVING expression: {expression.NodeType}.");
        }

        private static string TranslateHavingOperand(
            Expression expression,
            ParameterExpression groupParam,
            SelectQueryModel model,
            ISqlDialect dialect,
            List<object> parameters)
        {
            if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                return TranslateHavingOperand(unary.Operand, groupParam, model, dialect, parameters);
            }

            // Aggregate method calls: g.Count(), g.Sum(x => x.Prop), etc.
            if (expression is MethodCallExpression methodCall
                && methodCall.Method.DeclaringType == typeof(Enumerable))
            {
                return TranslateHavingAggregate(methodCall, groupParam, model, dialect);
            }

            // g.Key → group by column(s)
            if (expression is MemberExpression member
                && member.Member.Name == "Key"
                && member.Expression == groupParam)
            {
                if (model.GroupByColumns.Count == 1)
                {
                    return dialect.EscapeIdentifier(model.GroupByColumns[0].Name);
                }
                throw new NotSupportedException("Composite key reference in HAVING is not supported without a specific property.");
            }

            // g.Key.Prop → specific group by column for composite keys
            if (expression is MemberExpression outerMember
                && outerMember.Expression is MemberExpression innerMember
                && innerMember.Member.Name == "Key"
                && innerMember.Expression == groupParam)
            {
                var propName = outerMember.Member.Name;
                var col = model.Table.FindColumnWithPropertyName(propName);
                if (col != null)
                {
                    return dialect.EscapeIdentifier(col.Name);
                }
                throw new NotSupportedException($"Unknown property '{propName}' in HAVING clause.");
            }

            // Constant or captured value
            var value = EvaluateHavingValue(expression);
            return AddHavingParameter(dialect, parameters, value);
        }

        private static string TranslateHavingAggregate(
            MethodCallExpression methodCall,
            ParameterExpression groupParam,
            SelectQueryModel model,
            ISqlDialect dialect)
        {
            var methodName = methodCall.Method.Name;
            switch (methodName)
            {
                case nameof(Enumerable.Count):
                case nameof(Enumerable.LongCount):
                    return "count(*)";
                case "Sum":
                case "Min":
                case "Max":
                case "Average":
                {
                    var funcName = methodName == "Average" ? "avg" : methodName.ToLower();
                    if (methodCall.Arguments.Count < 2)
                    {
                        throw new NotSupportedException($"Aggregate {methodName} requires a selector in HAVING context.");
                    }

                    var selectorExpr = methodCall.Arguments[1];
                    while (selectorExpr.NodeType == ExpressionType.Quote)
                    {
                        selectorExpr = ((UnaryExpression)selectorExpr).Operand;
                    }

                    if (!(selectorExpr is LambdaExpression selectorLambda))
                    {
                        throw new NotSupportedException($"Expected lambda for aggregate {methodName}.");
                    }

                    var selectorBody = selectorLambda.Body;
                    if (selectorBody is UnaryExpression selectorUnary && selectorUnary.NodeType == ExpressionType.Convert)
                    {
                        selectorBody = selectorUnary.Operand;
                    }

                    if (selectorBody is MemberExpression selectorMember && selectorMember.Expression?.NodeType == ExpressionType.Parameter)
                    {
                        var col = model.Table.FindColumnWithPropertyName(selectorMember.Member.Name);
                        if (col != null)
                        {
                            return funcName + "(" + dialect.EscapeIdentifier(col.Name) + ")";
                        }
                    }

                    throw new NotSupportedException($"Unsupported aggregate selector in HAVING: {selectorExpr}.");
                }
                default:
                    throw new NotSupportedException($"Unsupported aggregate method in HAVING: {methodName}.");
            }
        }

        private static string AddHavingParameter(ISqlDialect dialect, List<object> parameters, object value)
        {
            var paramBaseName = "p" + (parameters.Count + 1).ToString();
            var parameterName = dialect.FormatParameterName(paramBaseName);
            parameters.Add(value);
            return parameterName;
        }

        private static object EvaluateHavingValue(Expression expression)
        {
            switch (expression)
            {
                case ConstantExpression constant:
                    return constant.Value;
                case MemberExpression member when member.Expression != null:
                    var obj = EvaluateHavingValue(member.Expression);
                    if (member.Member is System.Reflection.FieldInfo field)
                        return field.GetValue(obj);
                    if (member.Member is System.Reflection.PropertyInfo prop)
                        return prop.GetValue(obj);
                    break;
                case UnaryExpression unary when unary.NodeType == ExpressionType.Convert:
                    return EvaluateHavingValue(unary.Operand);
            }

            var lambda = Expression.Lambda(expression);
            return lambda.Compile().DynamicInvoke();
        }

        private static string ToSqlOperator(ExpressionType expressionType)
        {
            switch (expressionType)
            {
                case ExpressionType.Equal: return "=";
                case ExpressionType.NotEqual: return "!=";
                case ExpressionType.GreaterThan: return ">";
                case ExpressionType.GreaterThanOrEqual: return ">=";
                case ExpressionType.LessThan: return "<";
                case ExpressionType.LessThanOrEqual: return "<=";
                default:
                    throw new NotSupportedException($"Unsupported operator: {expressionType}.");
            }
        }
    }
}
