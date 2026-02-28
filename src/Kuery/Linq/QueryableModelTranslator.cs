using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kuery.Linq
{
    internal sealed class QueryableModelTranslator
    {
        internal SelectQueryModel Translate(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return TranslateCore(expression);
        }

        private SelectQueryModel TranslateCore(Expression expression)
        {
            if (expression is ConstantExpression constantExpression)
            {
                if (constantExpression.Value is IQueryable queryable)
                {
                    var map = SqlMapper.GetMapping(queryable.ElementType);
                    return new SelectQueryModel(map);
                }

                throw new NotSupportedException($"Unsupported query root constant: {expression.Type}");
            }

            if (expression is MethodCallExpression methodCall && methodCall.Method.DeclaringType == typeof(Queryable))
            {
                // Special case: SelectMany on GroupJoin → LEFT JOIN pattern
                if (methodCall.Method.Name == nameof(Queryable.SelectMany)
                    && methodCall.Arguments.Count == 3
                    && methodCall.Arguments[0] is MethodCallExpression innerCall
                    && innerCall.Method.DeclaringType == typeof(Queryable)
                    && innerCall.Method.Name == nameof(Queryable.GroupJoin))
                {
                    return TranslateLeftJoin(innerCall, methodCall);
                }

                var model = TranslateCore(methodCall.Arguments[0]);
                ApplyMethod(model, methodCall);
                return model;
            }

            throw new NotSupportedException($"Unsupported query expression: {expression}");
        }

        private SelectQueryModel TranslateLeftJoin(
            MethodCallExpression groupJoinCall,
            MethodCallExpression selectManyCall)
        {
            // GroupJoin(outer, inner, outerKey, innerKey, groupResultSelector)
            var model = TranslateCore(groupJoinCall.Arguments[0]);
            var innerTable = GetInnerTable(groupJoinCall.Arguments[1]);

            var outerKeyLambda = GetLambda(groupJoinCall.Arguments[2]);
            var innerKeyLambda = GetLambda(groupJoinCall.Arguments[3]);
            var groupResultLambda = GetLambda(groupJoinCall.Arguments[4]);

            var keyPairs = BuildKeyPairs(model, outerKeyLambda, innerTable, innerKeyLambda);

            // SelectMany(groupJoinResult, collectionSelector, resultSelector)
            var resultSelectorLambda = GetLambda(selectManyCall.Arguments[2]);

            // Find outer member name in GroupJoin result selector: (o, ols) => new { o, ols }
            var outerMemberName = GetOuterEntityMemberName(groupResultLambda, model.Table.MappedType);

            // Rewrite (x, inner) => ... where x.outerMemberName.Prop → outerParam.Prop
            var rewritten = RewriteLeftJoinSelector(
                resultSelectorLambda,
                resultSelectorLambda.Parameters[0],
                outerMemberName,
                model.Table.MappedType);

            var joinClause = new JoinClause(innerTable, keyPairs, rewritten, isLeftJoin: true);
            model.AddJoin(joinClause);
            model.SetJoinResultSelector(rewritten);
            return model;
        }

        private static void ApplyMethod(SelectQueryModel model, MethodCallExpression methodCall)
        {
            var name = methodCall.Method.Name;
            switch (name)
            {
                case nameof(Queryable.Where):
                    if (model.GroupByColumns != null && model.GroupByColumns.Count > 0)
                    {
                        var havingLambda = GetLambda(methodCall.Arguments[1]);
                        model.AddHavingPredicate(havingLambda.Body, havingLambda.Parameters[0]);
                    }
                    else
                    {
                        model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    }
                    break;
                case nameof(Queryable.OrderBy):
                    AddOrdering(model, GetLambda(methodCall.Arguments[1]), true);
                    break;
                case nameof(Queryable.OrderByDescending):
                    AddOrdering(model, GetLambda(methodCall.Arguments[1]), false);
                    break;
                case nameof(Queryable.ThenBy):
                    AddOrdering(model, GetLambda(methodCall.Arguments[1]), true);
                    break;
                case nameof(Queryable.ThenByDescending):
                    AddOrdering(model, GetLambda(methodCall.Arguments[1]), false);
                    break;
                case nameof(Queryable.Skip):
                    model.Skip = GetIntValue(methodCall.Arguments[1]);
                    break;
                case nameof(Queryable.Take):
                    model.Take = GetIntValue(methodCall.Arguments[1]);
                    break;
                case nameof(Queryable.Count):
                    model.SetTerminal(QueryTerminalKind.Count);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    }
                    break;
                case nameof(Queryable.LongCount):
                    model.SetTerminal(QueryTerminalKind.LongCount);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    }
                    break;
                case nameof(Queryable.Any):
                    model.SetTerminal(QueryTerminalKind.Any);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    }
                    break;
                case nameof(Queryable.All):
                    model.SetTerminal(QueryTerminalKind.All);
                    model.SetAllPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    break;
                case nameof(Queryable.First):
                    model.SetTerminal(QueryTerminalKind.First);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    }
                    break;
                case nameof(Queryable.FirstOrDefault):
                    model.SetTerminal(QueryTerminalKind.FirstOrDefault);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    }
                    break;
                case nameof(Queryable.Last):
                    model.SetTerminal(QueryTerminalKind.Last);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    }
                    break;
                case nameof(Queryable.LastOrDefault):
                    model.SetTerminal(QueryTerminalKind.LastOrDefault);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    }
                    break;
                case nameof(Queryable.Select):
                    if (model.GroupByColumns != null && model.GroupByColumns.Count > 0)
                    {
                        ApplyGroupBySelect(model, GetLambda(methodCall.Arguments[1]));
                    }
                    else
                    {
                        ApplySelect(model, GetLambda(methodCall.Arguments[1]));
                    }
                    break;
                case nameof(Queryable.Join):
                    ApplyJoin(model, methodCall);
                    break;
                case nameof(Queryable.Distinct):
                    model.IsDistinct = true;
                    break;
                case nameof(Queryable.GroupBy):
                    ApplyGroupBy(model, methodCall);
                    break;
                case "ElementAt":
                    model.SetTerminal(QueryTerminalKind.ElementAt);
                    model.Skip = (model.Skip ?? 0) + GetIntValue(methodCall.Arguments[1]);
                    break;
                case "ElementAtOrDefault":
                    model.SetTerminal(QueryTerminalKind.ElementAtOrDefault);
                    model.Skip = (model.Skip ?? 0) + GetIntValue(methodCall.Arguments[1]);
                    break;
                case nameof(Queryable.Single):
                    model.SetTerminal(QueryTerminalKind.Single);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    }
                    break;
                case nameof(Queryable.SingleOrDefault):
                    model.SetTerminal(QueryTerminalKind.SingleOrDefault);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
                    }
                    break;
                case nameof(Queryable.Sum):
                    model.SetTerminal(QueryTerminalKind.Sum);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.SetAggregateSelector(GetLambda(methodCall.Arguments[1]));
                    }
                    break;
                case nameof(Queryable.Min):
                    model.SetTerminal(QueryTerminalKind.Min);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.SetAggregateSelector(GetLambda(methodCall.Arguments[1]));
                    }
                    break;
                case nameof(Queryable.Max):
                    model.SetTerminal(QueryTerminalKind.Max);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.SetAggregateSelector(GetLambda(methodCall.Arguments[1]));
                    }
                    break;
                case nameof(Queryable.Average):
                    model.SetTerminal(QueryTerminalKind.Average);
                    if (methodCall.Arguments.Count == 2)
                    {
                        model.SetAggregateSelector(GetLambda(methodCall.Arguments[1]));
                    }
                    break;
                default:
                    throw new NotSupportedException(
                        $"Unsupported Queryable method: {name}.");
            }
        }

        private static void ApplySelect(SelectQueryModel model, LambdaExpression lambda)
        {
            var body = lambda.Body;
            var columns = new List<ProjectedColumn>();

            if (body is MemberExpression singleMember && singleMember.Expression?.NodeType == ExpressionType.Parameter)
            {
                var col = model.Table.FindColumnWithPropertyName(singleMember.Member.Name);
                if (col == null)
                {
                    throw new NotSupportedException($"Unknown property '{singleMember.Member.Name}' in Select.");
                }
                columns.Add(new ProjectedColumn(col, singleMember.Member.Name));
            }
            else if (body is NewExpression newExpr)
            {
                for (var i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    if (arg is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
                    {
                        arg = unary.Operand;
                    }

                    if (!(arg is MemberExpression member) || member.Expression?.NodeType != ExpressionType.Parameter)
                    {
                        throw new NotSupportedException($"Unsupported Select expression: {arg}. Only direct member access is supported.");
                    }

                    var col = model.Table.FindColumnWithPropertyName(member.Member.Name);
                    if (col == null)
                    {
                        throw new NotSupportedException($"Unknown property '{member.Member.Name}' in Select.");
                    }

                    var targetName = newExpr.Members != null ? newExpr.Members[i].Name : member.Member.Name;
                    columns.Add(new ProjectedColumn(col, targetName));
                }
            }
            else if (body is MemberInitExpression memberInit)
            {
                foreach (var binding in memberInit.Bindings)
                {
                    if (!(binding is MemberAssignment assignment))
                    {
                        throw new NotSupportedException($"Unsupported binding in Select: {binding.BindingType}. Only MemberAssignment is supported.");
                    }

                    var arg = assignment.Expression;
                    if (arg is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
                    {
                        arg = unary.Operand;
                    }

                    if (!(arg is MemberExpression member) || member.Expression?.NodeType != ExpressionType.Parameter)
                    {
                        throw new NotSupportedException($"Unsupported Select expression: {arg}. Only direct member access is supported.");
                    }

                    var col = model.Table.FindColumnWithPropertyName(member.Member.Name);
                    if (col == null)
                    {
                        throw new NotSupportedException($"Unknown property '{member.Member.Name}' in Select.");
                    }

                    columns.Add(new ProjectedColumn(col, assignment.Member.Name));
                }
            }
            else
            {
                throw new NotSupportedException($"Unsupported Select body: {body.NodeType}. Only new {{ }}, member init, and single member access are supported.");
            }

            model.SetProjection(lambda, columns);
        }

        private static void ApplyGroupBy(SelectQueryModel model, MethodCallExpression methodCall)
        {
            var lambda = GetLambda(methodCall.Arguments[1]);
            var body = lambda.Body;
            if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                body = unary.Operand;
            }

            if (body is MemberExpression member && member.Expression?.NodeType == ExpressionType.Parameter)
            {
                var col = model.Table.FindColumnWithPropertyName(member.Member.Name);
                if (col == null)
                {
                    throw new NotSupportedException($"Unknown property '{member.Member.Name}' in GroupBy.");
                }
                model.AddGroupByColumn(col);
            }
            else if (body is NewExpression newExpr)
            {
                for (var i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    if (arg is UnaryExpression argUnary && argUnary.NodeType == ExpressionType.Convert)
                    {
                        arg = argUnary.Operand;
                    }

                    if (!(arg is MemberExpression argMember) || argMember.Expression?.NodeType != ExpressionType.Parameter)
                    {
                        throw new NotSupportedException($"Unsupported GroupBy expression: {arg}. Only direct member access is supported.");
                    }

                    var col = model.Table.FindColumnWithPropertyName(argMember.Member.Name);
                    if (col == null)
                    {
                        throw new NotSupportedException($"Unknown property '{argMember.Member.Name}' in GroupBy.");
                    }
                    model.AddGroupByColumn(col);
                }
                model.GroupByKeyMembers = newExpr.Members;
            }
            else
            {
                throw new NotSupportedException($"Unsupported GroupBy expression: {lambda}. Only direct member access is supported.");
            }
        }

        private static void ApplyGroupBySelect(SelectQueryModel model, LambdaExpression lambda)
        {
            var body = lambda.Body;
            if (body is NewExpression newExpr)
            {
                var items = new List<GroupBySelectItem>();
                for (var i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    if (arg is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
                    {
                        arg = unary.Operand;
                    }

                    var targetName = newExpr.Members != null ? newExpr.Members[i].Name : "Item" + i;

                    if (arg is MemberExpression memberExpr
                        && memberExpr.Member.Name == "Key"
                        && memberExpr.Expression?.NodeType == ExpressionType.Parameter)
                    {
                        items.Add(new GroupBySelectItem(model.GroupByColumns[0], targetName));
                    }
                    else if (arg is MemberExpression outerMember
                        && outerMember.Expression is MemberExpression innerMember
                        && innerMember.Member.Name == "Key"
                        && innerMember.Expression?.NodeType == ExpressionType.Parameter)
                    {
                        // Composite key: g.Key.Prop
                        var propName = outerMember.Member.Name;
                        var col = FindGroupByColumn(model, propName);
                        if (col == null)
                        {
                            throw new NotSupportedException($"Unknown property '{propName}' in GroupBy composite key.");
                        }
                        items.Add(new GroupBySelectItem(col, targetName));
                    }
                    else if (arg is MethodCallExpression methodCall)
                    {
                        items.Add(TranslateGroupByAggregate(model, methodCall, targetName));
                    }
                    else
                    {
                        throw new NotSupportedException($"Unsupported expression in GroupBy Select: {arg}");
                    }
                }
                model.SetGroupBySelect(items, newExpr.Constructor);
            }
            else
            {
                throw new NotSupportedException($"Unsupported GroupBy Select body: {body.NodeType}. Only new {{ }} is supported.");
            }
        }

        private static TableMapping.Column FindGroupByColumn(SelectQueryModel model, string propertyName)
        {
            if (model.GroupByKeyMembers != null)
            {
                for (var i = 0; i < model.GroupByKeyMembers.Count; i++)
                {
                    if (model.GroupByKeyMembers[i].Name == propertyName && i < model.GroupByColumns.Count)
                    {
                        return model.GroupByColumns[i];
                    }
                }
            }
            return model.Table.FindColumnWithPropertyName(propertyName);
        }

        private static GroupBySelectItem TranslateGroupByAggregate(SelectQueryModel model, MethodCallExpression methodCall, string targetName)
        {
            var method = methodCall.Method;
            if (method.DeclaringType != typeof(Enumerable))
            {
                throw new NotSupportedException($"Unsupported method in GroupBy Select: {method.DeclaringType?.Name}.{method.Name}");
            }

            switch (method.Name)
            {
                case nameof(Enumerable.Count):
                case nameof(Enumerable.LongCount):
                    return new GroupBySelectItem("count", null, targetName);
                case "Sum":
                case "Min":
                case "Max":
                case "Average":
                {
                    var funcName = method.Name == "Average" ? "avg" : method.Name.ToLower();
                    if (methodCall.Arguments.Count < 2)
                    {
                        throw new NotSupportedException($"Aggregate {method.Name} requires a selector in GroupBy context.");
                    }

                    var selectorExpr = methodCall.Arguments[1];
                    while (selectorExpr.NodeType == ExpressionType.Quote)
                    {
                        selectorExpr = ((UnaryExpression)selectorExpr).Operand;
                    }

                    if (!(selectorExpr is LambdaExpression selectorLambda))
                    {
                        throw new NotSupportedException($"Expected lambda for aggregate {method.Name}.");
                    }

                    var selectorBody = selectorLambda.Body;
                    if (selectorBody is UnaryExpression selectorUnary && selectorUnary.NodeType == ExpressionType.Convert)
                    {
                        selectorBody = selectorUnary.Operand;
                    }

                    if (selectorBody is MemberExpression member && member.Expression?.NodeType == ExpressionType.Parameter)
                    {
                        var col = model.Table.FindColumnWithPropertyName(member.Member.Name);
                        if (col == null)
                        {
                            throw new NotSupportedException($"Unknown property '{member.Member.Name}' in aggregate.");
                        }
                        return new GroupBySelectItem(funcName, col, targetName);
                    }

                    throw new NotSupportedException($"Unsupported aggregate selector: {selectorExpr}. Only direct member access is supported.");
                }
                default:
                    throw new NotSupportedException($"Unsupported aggregate method in GroupBy: {method.Name}");
            }
        }

        private static void ApplyJoin(SelectQueryModel model, MethodCallExpression methodCall)
        {
            var innerTable = GetInnerTable(methodCall.Arguments[1]);

            var outerKeyLambda = GetLambda(methodCall.Arguments[2]);
            var innerKeyLambda = GetLambda(methodCall.Arguments[3]);
            var resultSelector = GetLambda(methodCall.Arguments[4]);

            var keyPairs = BuildKeyPairs(model, outerKeyLambda, innerTable, innerKeyLambda);
            var joinClause = new JoinClause(innerTable, keyPairs, resultSelector);
            model.AddJoin(joinClause);

            // For chained joins (JoinShape already exists from a previous join), rewrite the
            // result selector so the executor can call it with individual entity instances.
            LambdaExpression finalResultSelector;
            if (model.JoinShape != null && model.JoinShape.Count > 0)
            {
                finalResultSelector = RewriteMultiJoinSelector(resultSelector, model.JoinShape);
            }
            else
            {
                finalResultSelector = resultSelector;
            }
            model.SetJoinResultSelector(finalResultSelector);

            // Update JoinShape AFTER the rewrite, so future chained joins can resolve outer keys.
            UpdateJoinShape(model, resultSelector, innerTable);
        }

        private static TableMapping GetInnerTable(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote)
            {
                expression = ((UnaryExpression)expression).Operand;
            }

            if (expression is ConstantExpression constExpr && constExpr.Value is IQueryable innerQueryable)
            {
                return SqlMapper.GetMapping(innerQueryable.ElementType);
            }

            throw new NotSupportedException("Join inner source must be a root query.");
        }

        /// <summary>
        /// Builds a list of key pairs from the outer and inner key selectors.
        /// Handles both single-key (x => x.Id) and composite-key (x => new { x.A, x.B }) selectors.
        /// </summary>
        private static IReadOnlyList<JoinKeyPair> BuildKeyPairs(
            SelectQueryModel model,
            LambdaExpression outerKeyLambda,
            TableMapping innerTable,
            LambdaExpression innerKeyLambda)
        {
            var outerBody = UnwrapConvert(outerKeyLambda.Body);
            var innerBody = UnwrapConvert(innerKeyLambda.Body);

            // Composite key: both sides return anonymous types
            if (outerBody is NewExpression outerNew && innerBody is NewExpression innerNew)
            {
                if (outerNew.Arguments.Count != innerNew.Arguments.Count)
                {
                    throw new NotSupportedException(
                        $"Composite join keys must have the same number of members. " +
                        $"Outer has {outerNew.Arguments.Count} member(s) but inner has {innerNew.Arguments.Count}.");
                }

                var pairs = new List<JoinKeyPair>(outerNew.Arguments.Count);
                for (var i = 0; i < outerNew.Arguments.Count; i++)
                {
                    var outerTable = ResolveOuterTable(model, UnwrapConvert(outerNew.Arguments[i]), out var outerCol);
                    var innerCol = GetColumnFromMember(innerTable, UnwrapConvert(innerNew.Arguments[i]));
                    pairs.Add(new JoinKeyPair(outerTable, outerCol, innerCol));
                }

                return pairs;
            }

            // Single key
            {
                var outerTable = ResolveOuterTable(model, outerBody, out var outerCol);
                var innerCol = GetColumnFromMember(innerTable, innerBody);
                return new[] { new JoinKeyPair(outerTable, outerCol, innerCol) };
            }
        }

        /// <summary>
        /// Resolves which table an outer key expression belongs to and returns the column.
        /// Handles both simple (param.Prop) and chained-join (x.member.Prop) forms.
        /// </summary>
        private static TableMapping ResolveOuterTable(
            SelectQueryModel model,
            Expression body,
            out TableMapping.Column column)
        {
            if (body is MemberExpression member)
            {
                // Simple: outerParam.Prop → outer table
                if (member.Expression?.NodeType == ExpressionType.Parameter)
                {
                    var col = model.Table.FindColumnWithPropertyName(member.Member.Name);
                    if (col != null)
                    {
                        column = col;
                        return model.Table;
                    }
                }

                // Chained join: x.joinMember.Prop where joinMember is in JoinShape
                if (member.Expression is MemberExpression outerAccess
                    && outerAccess.Expression?.NodeType == ExpressionType.Parameter
                    && model.JoinShape != null
                    && model.JoinShape.TryGetValue(outerAccess.Member.Name, out var joinedTable))
                {
                    var col = joinedTable.FindColumnWithPropertyName(member.Member.Name);
                    if (col != null)
                    {
                        column = col;
                        return joinedTable;
                    }
                }
            }

            throw new NotSupportedException(
                $"Unsupported outer key expression: {body}. Only direct member access or chained join member access is supported.");
        }

        private static TableMapping.Column GetColumnFromMember(TableMapping table, Expression body)
        {
            if (body is MemberExpression member && member.Expression?.NodeType == ExpressionType.Parameter)
            {
                var col = table.FindColumnWithPropertyName(member.Member.Name);
                if (col != null)
                {
                    return col;
                }
            }

            throw new NotSupportedException(
                $"Unsupported key expression: {body}. Only direct member access is supported.");
        }

        private static Expression UnwrapConvert(Expression expr)
        {
            while (expr is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                expr = unary.Operand;
            }

            return expr;
        }

        /// <summary>
        /// Updates the JoinShape from a join result selector so subsequent joins can
        /// resolve outer key expressions like x.member.Prop.
        /// </summary>
        private static void UpdateJoinShape(
            SelectQueryModel model,
            LambdaExpression resultSelector,
            TableMapping newInnerTable)
        {
            if (!(resultSelector.Body is NewExpression newExpr) || newExpr.Members == null)
            {
                return;
            }

            var firstParam = resultSelector.Parameters[0];
            var secondParam = resultSelector.Parameters.Count > 1 ? resultSelector.Parameters[1] : null;

            // Build new shape (may replace previous)
            var newShape = new Dictionary<string, TableMapping>();

            for (var i = 0; i < newExpr.Arguments.Count; i++)
            {
                var arg = UnwrapConvert(newExpr.Arguments[i]);
                var memberName = newExpr.Members[i].Name;

                if (arg is ParameterExpression param)
                {
                    if (param == firstParam)
                    {
                        // First join: param is the outer entity directly
                        newShape[memberName] = model.Table;
                    }
                    else if (param == secondParam)
                    {
                        newShape[memberName] = newInnerTable;
                    }
                }
                else if (arg is MemberExpression memberAccess && memberAccess.Expression == firstParam)
                {
                    // Subsequent join: x.prevMember → look up in existing shape
                    if (model.JoinShape != null
                        && model.JoinShape.TryGetValue(memberAccess.Member.Name, out var prevTable))
                    {
                        newShape[memberName] = prevTable;
                    }
                }
            }

            foreach (var kv in newShape)
            {
                model.SetJoinShapeMember(kv.Key, kv.Value);
            }
        }

        /// <summary>
        /// For multiple chained joins, rewrites the final result selector
        /// from (x, inner) => ... to (t1, t2, ..., inner) => ... where ti are entity params.
        /// </summary>
        private static LambdaExpression RewriteMultiJoinSelector(
            LambdaExpression selector,
            Dictionary<string, TableMapping> joinShape)
        {
            var firstParam = selector.Parameters[0];
            var innerParam = selector.Parameters[1];

            // Build ordered list of (memberName, table) from shape (preserve insertion order)
            var tableParams = new Dictionary<string, ParameterExpression>(StringComparer.Ordinal);
            foreach (var kv in joinShape)
            {
                tableParams[kv.Key] = Expression.Parameter(kv.Value.MappedType, kv.Key);
            }

            var rewriter = new MultiJoinRewriter(firstParam, tableParams);
            var newBody = rewriter.Visit(selector.Body);

            var newParams = new List<ParameterExpression>(tableParams.Values) { innerParam };
            return Expression.Lambda(newBody, newParams);
        }

        /// <summary>
        /// For LEFT JOIN: rewrites (x, inner) => ... where x.outerMember.Prop
        /// to (outerParam, inner) => ... where outerParam.Prop.
        /// </summary>
        private static LambdaExpression RewriteLeftJoinSelector(
            LambdaExpression selector,
            ParameterExpression xParam,
            string outerMemberName,
            Type outerEntityType)
        {
            var outerParam = Expression.Parameter(outerEntityType, "outer");
            var innerParam = selector.Parameters[1];

            var rewriter = new LeftJoinRewriter(xParam, outerMemberName, outerParam);
            var newBody = rewriter.Visit(selector.Body);

            return Expression.Lambda(newBody, outerParam, innerParam);
        }

        /// <summary>
        /// Returns the member name in the GroupJoin result selector that holds the outer entity.
        /// E.g., for (o, ols) => new { o, ols }, returns "o".
        /// </summary>
        private static string GetOuterEntityMemberName(LambdaExpression groupResultLambda, Type outerEntityType)
        {
            if (groupResultLambda.Body is NewExpression newExpr && newExpr.Members != null)
            {
                for (var i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = UnwrapConvert(newExpr.Arguments[i]);
                    if (arg is ParameterExpression param && param.Type == outerEntityType)
                    {
                        return newExpr.Members[i].Name;
                    }
                }
            }

            throw new NotSupportedException(
                "Cannot determine outer entity member from GroupJoin result selector. " +
                $"Expected pattern: (outer, items) => new {{ outer, items }}. Got: {groupResultLambda}");
        }

        // ------------------------------------------------------------------
        // Expression rewriters
        // ------------------------------------------------------------------

        /// <summary>
        /// Rewrites x.memberName → corresponding table param (for multi-join selectors).
        /// Also handles x.memberName.Prop → tableParam.Prop.
        /// </summary>
        private sealed class MultiJoinRewriter : ExpressionVisitor
        {
            private readonly ParameterExpression _firstParam;
            private readonly Dictionary<string, ParameterExpression> _memberToParam;

            internal MultiJoinRewriter(
                ParameterExpression firstParam,
                Dictionary<string, ParameterExpression> memberToParam)
            {
                _firstParam = firstParam;
                _memberToParam = memberToParam;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                var inner = UnwrapConvert(node.Expression);

                // x.memberName.Prop → tableParam.Prop
                if (inner is MemberExpression innerMember
                    && innerMember.Expression == _firstParam
                    && _memberToParam.TryGetValue(innerMember.Member.Name, out var param1))
                {
                    var prop = GetMemberInfo(param1.Type, node.Member.Name);
                    if (prop != null) return Expression.MakeMemberAccess(param1, prop);
                }

                // x.memberName → tableParam (entire entity)
                if (node.Expression == _firstParam
                    && _memberToParam.TryGetValue(node.Member.Name, out var param2))
                {
                    return param2;
                }

                return base.VisitMember(node);
            }
        }

        /// <summary>
        /// Rewrites x.outerMember.Prop → outerParam.Prop (for LEFT JOIN selectors).
        /// </summary>
        private sealed class LeftJoinRewriter : ExpressionVisitor
        {
            private readonly ParameterExpression _xParam;
            private readonly string _outerMemberName;
            private readonly ParameterExpression _outerParam;

            internal LeftJoinRewriter(
                ParameterExpression xParam,
                string outerMemberName,
                ParameterExpression outerParam)
            {
                _xParam = xParam;
                _outerMemberName = outerMemberName;
                _outerParam = outerParam;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                var inner = UnwrapConvert(node.Expression);
                if (inner is MemberExpression outerAccess
                    && outerAccess.Member.Name == _outerMemberName
                    && outerAccess.Expression == _xParam)
                {
                    var prop = GetMemberInfo(_outerParam.Type, node.Member.Name);
                    if (prop != null) return Expression.MakeMemberAccess(_outerParam, prop);
                }

                return base.VisitMember(node);
            }
        }

        private static MemberInfo GetMemberInfo(Type type, string name)
        {
            return (MemberInfo)type.GetProperty(name)
                ?? (MemberInfo)type.GetField(name);
        }

        private static void AddOrdering(SelectQueryModel model, LambdaExpression lambda, bool ascending)
        {
            var body = lambda.Body;
            if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                body = unary.Operand;
            }

            if (!(body is MemberExpression member) || member.Expression?.NodeType != ExpressionType.Parameter)
            {
                throw new NotSupportedException($"Unsupported ordering expression: {lambda}. Only direct member access is supported.");
            }

            var column = model.Table.FindColumnWithPropertyName(member.Member.Name);
            if (column == null)
            {
                throw new NotSupportedException($"Unknown property '{member.Member.Name}' for ordering.");
            }

            model.AddOrdering(column, ascending);
        }

        private static int GetIntValue(Expression expression)
        {
            var value = EvaluateValue(expression);
            if (value is int intValue)
            {
                return intValue;
            }

            throw new NotSupportedException($"Expected integer argument but got {value?.GetType().Name ?? "null"}.");
        }

        private static object EvaluateValue(Expression expression)
        {
            var lambda = Expression.Lambda(expression);
            return lambda.Compile().DynamicInvoke();
        }

        private static LambdaExpression GetLambda(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote)
            {
                expression = ((UnaryExpression)expression).Operand;
            }

            if (expression is LambdaExpression lambda)
            {
                return lambda;
            }

            throw new NotSupportedException($"Expected lambda expression but got {expression.NodeType}.");
        }
    }
}
