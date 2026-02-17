using System;
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
                default:
                    throw new NotSupportedException(
                        $"Unsupported Queryable method: {name}. Supported methods: Where, OrderBy, OrderByDescending, ThenBy, ThenByDescending, Skip, Take, Count, First, FirstOrDefault.");
            }
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
