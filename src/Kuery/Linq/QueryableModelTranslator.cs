using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

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
                var model = TranslateCore(methodCall.Arguments[0]);
                ApplyMethod(model, methodCall);
                return model;
            }

            throw new NotSupportedException($"Unsupported query expression: {expression}");
        }

        private static void ApplyMethod(SelectQueryModel model, MethodCallExpression methodCall)
        {
            var name = methodCall.Method.Name;
            switch (name)
            {
                case nameof(Queryable.Where):
                    model.AddPredicate(GetLambda(methodCall.Arguments[1]).Body);
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
                case nameof(Queryable.Select):
                    ApplySelect(model, GetLambda(methodCall.Arguments[1]));
                    break;
                case nameof(Queryable.Distinct):
                    model.IsDistinct = true;
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
