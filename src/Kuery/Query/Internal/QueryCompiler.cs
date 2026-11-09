using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    internal sealed class QueryCompiler
    {
        internal RelationalCommand CompileQuery(QueryContext context, Expression expression, Type resultType, bool async,
            out CompiledQuery plan, string operation = "select", IReadOnlyList<SetPropertyCall> setters = null)
        {
            var normalizer = new ExpressionTreeFuncletizer(context.Connection);
            var normalized = normalizer.ExtractParameters(expression);
            var dialect = SqlDialectFactory.Create(context.Connection);
            var key = CompiledQueryCacheKeyGenerator.GenerateCacheKey(normalized, dialect.Kind, resultType, operation, async);
            List<SetPropertyCall> normalizedSetters = null;
            if (setters != null)
            {
                normalizedSetters = new List<SetPropertyCall>();
                foreach (var setter in setters)
                {
                    var property = (LambdaExpression)normalizer.Visit(setter.PropertyExpression);
                    var value = (LambdaExpression)normalizer.Visit(setter.ValueExpression);
                    normalizedSetters.Add(new SetPropertyCall(property, value));
                    key += CompiledQueryCacheKeyGenerator.GenerateCacheKey(property, dialect.Kind, resultType, "property", async)
                        + CompiledQueryCacheKeyGenerator.GenerateCacheKey(value, dialect.Kind, resultType, "value", async);
                }
            }
            plan = CompiledQueryCache.GetOrAdd(key, () => operation == "select"
                ? new QueryCompilationContext().CreateQueryExecutor(normalized)
                : new QueryCompilationContext().CreateNonQueryExecutor(normalized, operation, normalizedSetters));
            return RelationalCommandCache.GetRelationalCommand(plan, dialect, normalizer.Values);
        }

    }
}
