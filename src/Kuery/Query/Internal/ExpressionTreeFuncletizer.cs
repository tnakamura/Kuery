using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Kuery.Query;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    internal sealed class ExpressionTreeFuncletizer : ExpressionVisitor
    {
        readonly IDbConnection _connection;
        internal readonly List<object> Values = new List<object>();
        readonly bool _allowTags;
        List<string> _tags;
        internal IReadOnlyList<string> Tags => _tags;

        internal ExpressionTreeFuncletizer(IDbConnection connection, bool allowTags = true)
        {
            _connection = connection;
            _allowTags = allowTags;
        }

        internal Expression ExtractParameters(Expression expression)
        {
            Values.Clear();
            _tags?.Clear();
            return Visit(expression);
        }

        /// <summary>Replaces evaluatable values and query roots with normalized expressions.</summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The visited expression.</returns>
        public override Expression Visit(Expression node)
        {
            if (node == null || node is SqlExpression || node is QueryParameterExpression || node is QueryRootExpression) return node;
            if (node is QueryTagExpression tag)
            {
                if (!_allowTags) throw new NotSupportedException("Query tags are supported only for read queries.");
                if (_tags == null) _tags = new List<string>();
                var source = Visit(tag.Source);
                _tags.Add(tag.Tag);
                return source;
            }
            // .NET 10 may bind array.Contains to MemoryExtensions and insert a ref-struct conversion.
            // Normalize it before trying to parameterize a ReadOnlySpan<T>.
            if (node is MethodCallExpression contains && contains.Method.DeclaringType?.FullName == "System.MemoryExtensions"
                && contains.Method.Name == "Contains" && contains.Method.IsGenericMethod && contains.Arguments.Count == 2)
            {
                var source = contains.Arguments[0];
                if (source is MethodCallExpression conversion && conversion.Method.Name == "op_Implicit" && conversion.Arguments.Count == 1)
                    source = conversion.Arguments[0];
                if (!source.Type.IsArray) throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(node);
                return Visit(Expression.Call(typeof(Enumerable), nameof(Enumerable.Contains), contains.Method.GetGenericArguments(), source, contains.Arguments[1]));
            }
            if (node is MethodCallExpression fullJoin && fullJoin.Method.DeclaringType == typeof(Queryable)
                && fullJoin.Method.Name == "FullJoin" && fullJoin.Arguments.Count == 6)
            {
                if (!IsEvaluatable(fullJoin.Arguments[5]) || Evaluate(fullJoin.Arguments[5]) != null)
                    throw new NotSupportedException("FullJoin supports only the default comparer.");
                return fullJoin.Update(null, fullJoin.Arguments.Take(5).Select(Visit)
                    .Concat(new[] { Expression.Constant(null, fullJoin.Arguments[5].Type) }));
            }
            if (node is MethodCallExpression entityContains && entityContains.Method.DeclaringType == typeof(Queryable)
                && entityContains.Method.Name == nameof(Queryable.Contains) && entityContains.Arguments.Count == 2
                && entityContains.Method.GetGenericArguments()[0] != typeof(object)
                && !IsScalar(entityContains.Method.GetGenericArguments()[0]) && IsEvaluatable(entityContains.Arguments[1]))
            {
                var type = entityContains.Method.GetGenericArguments()[0];
                var mapping = SqlMapper.GetMapping(type);
                if (mapping.Columns.Count(c => c.IsPK) != 1) throw new NotSupportedException("Entity Contains requires a single mapped primary key: " + type);
                var value = Evaluate(entityContains.Arguments[1]);
                var keyType = mapping.PK.PropertyInfo.PropertyType;
                if (keyType.IsValueType && Nullable.GetUnderlyingType(keyType) == null) keyType = typeof(Nullable<>).MakeGenericType(keyType);
                var key = new QueryParameterExpression(Values.Count, keyType);
                Values.Add(value == null ? null : mapping.PK.PropertyInfo.GetValue(value, null));
                var present = new QueryParameterExpression(Values.Count, typeof(bool));
                Values.Add(value != null);
                return entityContains.Update(null, new[] { Visit(entityContains.Arguments[0]), new EntityReferenceExpression(type, key, present) });
            }
            var construction = node is MemberInitExpression || (node is NewExpression && !IsScalar(node.Type)
                && !typeof(System.Collections.IEnumerable).IsAssignableFrom(node.Type));
            if (!construction && !(node is LambdaExpression) && node.NodeType != ExpressionType.Quote && IsEvaluatable(node))
            {
                object value;
                try { value = Evaluate(node); }
                catch (Exception ex) { throw new NotSupportedException("Cannot evaluate query value: " + node, ex); }
                if (value is IQueryable query)
                {
                    if (!(query.Provider is KueryQueryProvider provider) || !ReferenceEquals(provider.Connection, _connection))
                        throw new NotSupportedException("Queries must use the same connection and the Kuery provider.");
                    if (query.Expression is ConstantExpression root && ReferenceEquals(root.Value, query))
                        return new QueryRootExpression(SqlMapper.GetMapping(query.ElementType));
                    return Visit(query.Expression);
                }
                var index = Values.Count;
                Values.Add(value);
                return new QueryParameterExpression(index, node.Type);
            }
            return base.Visit(node);
        }

        internal static bool IsScalar(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            return type.IsPrimitive || type.IsEnum || type == typeof(string) || type == typeof(decimal)
                || type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(Guid) || type == typeof(TimeSpan);
        }

        static object Evaluate(Expression expression)
        {
            if (expression is ConstantExpression constant) return constant.Value;
            if (expression is MemberExpression member)
            {
                var instance = member.Expression == null ? null : Evaluate(member.Expression);
                if (member.Member is FieldInfo field) return field.GetValue(instance);
                if (member.Member is PropertyInfo property) return property.GetValue(instance, null);
            }
            if (expression is MethodCallExpression call)
                return call.Method.Invoke(call.Object == null ? null : Evaluate(call.Object), call.Arguments.Select(Evaluate).ToArray());
            if (expression is NewExpression created) return created.Constructor == null ? Activator.CreateInstance(created.Type) : created.Constructor.Invoke(created.Arguments.Select(Evaluate).ToArray());
            // The interpreter handles independent arithmetic/conversions without generating a delegate per value.
            return Expression.Lambda<Func<object>>(Expression.Convert(expression, typeof(object))).Compile(preferInterpretation: true)();
        }

        private static bool IsEvaluatable(Expression expression)
        {
            var detector = new DependencyDetector();
            detector.Visit(expression);
            return !detector.Dependent;
        }

        private sealed class DependencyDetector : ExpressionVisitor
        {
            internal bool Dependent;
            /// <summary>Detects expressions that depend on a query or must remain in SQL.</summary>
            /// <param name="node">The expression to visit.</param>
            /// <returns>The visited expression.</returns>
            public override Expression Visit(Expression node)
            {
                if (node == null || Dependent) return node;
                if (node is FromSqlQueryRootExpression || node is QueryTagExpression || node is ParameterExpression || node is SqlExpression || node is QueryParameterExpression || node is QueryRootExpression
                    || node is LambdaExpression || node.NodeType == ExpressionType.Quote)
                { Dependent = true; return node; }
                if (node is MethodCallExpression call && (call.Method.DeclaringType == typeof(Queryable) || call.Method.DeclaringType == typeof(KueryFunctions) || call.Method.DeclaringType == typeof(KueryQueryableExtensions)))
                { Dependent = true; return node; }
                if (node is MemberExpression member && member.Member.DeclaringType == typeof(DateTime)
                    && (member.Member.Name == nameof(DateTime.Now) || member.Member.Name == nameof(DateTime.UtcNow) || member.Member.Name == nameof(DateTime.Today)))
                { Dependent = true; return node; }
                return base.Visit(node);
            }
        }
    }
}
