using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kuery
{
    public sealed class KueryQueryProvider : IQueryProvider
    {
        internal KueryQueryProvider(IDbConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        internal IDbConnection Connection { get; }

        public IQueryable CreateQuery(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));

            var elementType = expression.Type.GetGenericArguments()[0];
            var method = typeof(KueryQueryProvider)
                .GetMethod(nameof(CreateQueryCore), BindingFlags.Instance | BindingFlags.NonPublic)
                .MakeGenericMethod(elementType);
            return (IQueryable)method.Invoke(this, new object[] { expression });
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return new KueryQueryable<TElement>(this, expression);
        }

        public object Execute(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            var resultType = expression.Type;
            var method = typeof(KueryQueryProvider)
                .GetMethod(nameof(ExecuteCore), BindingFlags.Instance | BindingFlags.NonPublic)
                .MakeGenericMethod(resultType);
            return method.Invoke(this, new object[] { expression });
        }

        public TResult Execute<TResult>(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return ExecuteCore<TResult>(expression);
        }

        private IQueryable<TElement> CreateQueryCore<TElement>(Expression expression)
        {
            return new KueryQueryable<TElement>(this, expression);
        }

        private TResult ExecuteCore<TResult>(Expression expression)
        {
            try
            {
                object result;

                if (expression is MethodCallExpression methodCall &&
                    methodCall.Method.DeclaringType == typeof(Queryable) &&
                    IsTerminalMethod(methodCall.Method.Name))
                {
                    var query = BuildTableQuery(methodCall.Arguments[0]);
                    result = ExecuteTerminal(methodCall, query);
                }
                else
                {
                    result = BuildTableQuery(expression);
                }

                if (result is TResult typed)
                {
                    return typed;
                }

                return (TResult)Convert.ChangeType(result, typeof(TResult), CultureInfo.InvariantCulture);
            }
            catch (NotSupportedException)
            {
                return EvaluateOnClient<TResult>(expression);
            }
        }

        internal object ExecuteTerminal(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            if (!(expression is MethodCallExpression methodCall) || methodCall.Method.DeclaringType != typeof(Queryable))
            {
                throw new NotSupportedException($"Unsupported terminal expression: {expression.NodeType}");
            }

            var query = BuildTableQuery(methodCall.Arguments[0]);
            return ExecuteTerminal(methodCall, query);
        }

        internal object BuildTableQuery(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));

            if (expression is ConstantExpression constantExpression)
            {
                if (constantExpression.Value is IQueryable queryable && queryable.Provider == this)
                {
                    return CreateTableQuery(queryable.ElementType);
                }
            }

            if (expression is MethodCallExpression methodCall && methodCall.Method.DeclaringType == typeof(Queryable))
            {
                var source = BuildTableQuery(methodCall.Arguments[0]);
                return ApplySequenceMethod(source, methodCall);
            }

            throw new NotSupportedException($"Unsupported query expression: {expression}");
        }

        private static bool IsTerminalMethod(string methodName)
        {
            return methodName == nameof(Queryable.Count) ||
                   methodName == nameof(Queryable.First) ||
                   methodName == nameof(Queryable.FirstOrDefault);
        }

        private object ExecuteTerminal(MethodCallExpression methodCall, object query)
        {
            var methodName = methodCall.Method.Name;
            if (methodCall.Arguments.Count == 2)
            {
                var predicate = StripQuotes(methodCall.Arguments[1]);
                query = InvokeQueryMethod(query, nameof(Queryable.Where), predicate);
            }

            if (methodName == nameof(Queryable.Count) ||
                methodName == nameof(Queryable.First) ||
                methodName == nameof(Queryable.FirstOrDefault))
            {
                return InvokeParameterlessQueryMethod(query, methodName);
            }

            throw new NotSupportedException($"Unsupported terminal method: {methodName}");
        }

        private object ApplySequenceMethod(object source, MethodCallExpression methodCall)
        {
            var methodName = methodCall.Method.Name;
            switch (methodName)
            {
                case nameof(Queryable.Where):
                case nameof(Queryable.OrderBy):
                case nameof(Queryable.OrderByDescending):
                case nameof(Queryable.ThenBy):
                case nameof(Queryable.ThenByDescending):
                    return InvokeQueryMethod(source, methodName, StripQuotes(methodCall.Arguments[1]));

                case nameof(Queryable.Skip):
                case nameof(Queryable.Take):
                    return InvokeQueryMethod(source, methodName, EvaluateValue(methodCall.Arguments[1]));

                case nameof(Queryable.Count):
                case nameof(Queryable.First):
                case nameof(Queryable.FirstOrDefault):
                    return source;

                default:
                    throw new NotSupportedException($"Unsupported Queryable method: {methodName}");
            }
        }

        private static object EvaluateValue(Expression expression)
        {
            var lambda = Expression.Lambda(expression);
            return lambda.Compile().DynamicInvoke();
        }

        private object CreateTableQuery(Type elementType)
        {
            var tableQueryType = typeof(TableQuery<>).MakeGenericType(elementType);
            return Activator.CreateInstance(
                tableQueryType,
                BindingFlags.Instance | BindingFlags.NonPublic,
                binder: null,
                args: new object[] { Connection },
                culture: CultureInfo.InvariantCulture);
        }

        private TResult EvaluateOnClient<TResult>(Expression expression)
        {
            var visitor = new ClientFallbackVisitor(this);
            var rewritten = visitor.Visit(expression);
            var lambda = Expression.Lambda<Func<TResult>>(Expression.Convert(rewritten, typeof(TResult)));
            return lambda.Compile().Invoke();
        }

        private static Expression StripQuotes(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote)
            {
                expression = ((UnaryExpression)expression).Operand;
            }

            return expression;
        }

        private static object InvokeParameterlessQueryMethod(object source, string methodName)
        {
            var method = source.GetType().GetMethod(methodName, Type.EmptyTypes);
            if (method == null)
            {
                throw new NotSupportedException($"Method '{methodName}' is not supported for {source.GetType().Name}");
            }

            return method.Invoke(source, null);
        }

        private static object InvokeQueryMethod(object source, string methodName, object argument)
        {
            var methods = source.GetType().GetMethods()
                .Where(x => x.Name == methodName && x.GetParameters().Length == 1)
                .ToArray();
            if (methods.Length == 0)
            {
                throw new NotSupportedException($"Method '{methodName}' is not supported for {source.GetType().Name}");
            }

            MethodInfo method;
            if (argument is LambdaExpression lambda)
            {
                method = methods.FirstOrDefault(x => x.IsGenericMethodDefinition)
                    ?.MakeGenericMethod(lambda.Body.Type);

                if (method == null)
                {
                    method = methods.FirstOrDefault(x =>
                    {
                        if (x.IsGenericMethodDefinition)
                        {
                            return false;
                        }

                        var parameterType = x.GetParameters()[0].ParameterType;
                        return parameterType.IsAssignableFrom(lambda.GetType());
                    });
                }

                if (method == null)
                {
                    throw new NotSupportedException($"Method '{methodName}' does not accept argument type {lambda.GetType().Name}");
                }

                return method.Invoke(source, new object[] { lambda });
            }

            method = methods.FirstOrDefault(x => !x.IsGenericMethodDefinition && x.GetParameters()[0].ParameterType == argument.GetType())
                ?? methods.FirstOrDefault(x => !x.IsGenericMethodDefinition && x.GetParameters()[0].ParameterType.IsAssignableFrom(argument.GetType()));

            if (method == null)
            {
                throw new NotSupportedException($"Method '{methodName}' does not accept argument type {argument.GetType().Name}");
            }

            return method.Invoke(source, new[] { argument });
        }

        sealed class ClientFallbackVisitor : ExpressionVisitor
        {
            private readonly KueryQueryProvider _provider;

            internal ClientFallbackVisitor(KueryQueryProvider provider)
            {
                _provider = provider;
            }

            protected override Expression VisitConstant(ConstantExpression node)
            {
                if (node.Value is IQueryable queryable && queryable.Provider == _provider)
                {
                    var tableQuery = _provider.CreateTableQuery(queryable.ElementType);
                    var toList = tableQuery.GetType().GetMethod(nameof(TableQuery<int>.ToList), Type.EmptyTypes);
                    var enumerable = toList.Invoke(tableQuery, null);
                    var asQueryable = typeof(Queryable)
                        .GetMethods(BindingFlags.Public | BindingFlags.Static)
                        .First(x => x.Name == nameof(Queryable.AsQueryable) && x.IsGenericMethodDefinition)
                        .MakeGenericMethod(queryable.ElementType);
                    var inMemoryQueryable = asQueryable.Invoke(null, new[] { enumerable });
                    return Expression.Constant(inMemoryQueryable, node.Type);
                }

                return base.VisitConstant(node);
            }
        }
    }
}
