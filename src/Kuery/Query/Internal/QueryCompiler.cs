using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    internal sealed class QueryCompiler
    {
        internal RelationalCommand CompileQuery(QueryContext context, Expression expression, Type resultType, bool async,
            out CompiledQuery plan, string operation = "select", IReadOnlyList<SetPropertyCall> setters = null)
        {
            var normalizer = new ExpressionTreeFuncletizer(context.Connection, allowTags: operation == "select");
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
            var command = RelationalCommandCache.GetRelationalCommand(plan, dialect, normalizer.Values);
            command.CommandText = ApplyTags(command.CommandText, normalizer.Tags);
            return command;
        }

        static string ApplyTags(string sql, IReadOnlyList<string> tags)
        {
            if (tags == null || tags.Count == 0) return sql;

            var text = new StringBuilder();
            foreach (var tag in tags)
            {
                foreach (var line in tag.Replace("\r\n", "\n").Replace('\r', '\n').Split('\n'))
                {
                    text.Append("-- ");
                    foreach (var c in line)
                        text.Append(char.IsControl(c) || c == '\u2028' || c == '\u2029' ? ' ' : c);
                    text.Append('\n');
                }
            }
            return text.Append(sql).ToString();
        }

    }
}
