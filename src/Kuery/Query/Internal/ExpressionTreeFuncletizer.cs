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
        internal ExpressionTreeFuncletizer(IDbConnection connection)
        {
            _connection = connection;
        }

        internal Expression ExtractParameters(Expression expression)
        {
            Values.Clear();
            return Visit(expression);
        }

        public override Expression Visit(Expression node)
        {
            if (node == null || node is SqlExpression || node is QueryParameterExpression || node is QueryRootExpression) return node;
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

        static bool IsScalar(Type type)
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
            public override Expression Visit(Expression node)
            {
                if (node == null || Dependent) return node;
                if (node is ParameterExpression || node is SqlExpression || node is QueryParameterExpression || node is QueryRootExpression
                    || node is LambdaExpression || node.NodeType == ExpressionType.Quote)
                { Dependent = true; return node; }
                if (node is MethodCallExpression call && (call.Method.DeclaringType == typeof(Queryable) || call.Method.DeclaringType == typeof(KueryFunctions)))
                { Dependent = true; return node; }
                if (node is MemberExpression member && member.Member.DeclaringType == typeof(DateTime)
                    && (member.Member.Name == nameof(DateTime.Now) || member.Member.Name == nameof(DateTime.UtcNow) || member.Member.Name == nameof(DateTime.Today)))
                { Dependent = true; return node; }
                return base.Visit(node);
            }
        }
    }
}
