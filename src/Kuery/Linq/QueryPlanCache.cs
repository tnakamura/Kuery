using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Kuery.Linq
{
    internal sealed class QueryNormalizer : ExpressionVisitor
    {
        readonly IDbConnection _connection;
        internal readonly List<object> Values = new List<object>();
        internal QueryNormalizer(IDbConnection connection) { _connection = connection; }

        public override Expression Visit(Expression node)
        {
            if (node == null || node is SqlExpression || node is QueryRootExpression) return node;
            // .NET 10 may bind array.Contains to MemoryExtensions and insert a ref-struct conversion.
            // Normalize it before trying to parameterize a ReadOnlySpan<T>.
            if (node is MethodCallExpression contains && contains.Method.DeclaringType?.FullName == "System.MemoryExtensions"
                && contains.Method.Name == "Contains" && contains.Method.IsGenericMethod && contains.Arguments.Count == 2)
            {
                var source = contains.Arguments[0];
                if (source is MethodCallExpression conversion && conversion.Method.Name == "op_Implicit" && conversion.Arguments.Count == 1)
                    source = conversion.Arguments[0];
                if (!source.Type.IsArray) throw RelationalQueryCompiler.Unsupported(node);
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
                return new SqlParameterExpression(index, node.Type);
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
                if (node is ParameterExpression || node is SqlExpression || node is QueryRootExpression
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

    internal static class QueryPlanCache
    {
        const int Capacity = 256;
        static readonly object Gate = new object();
        static readonly Dictionary<string, LinkedListNode<KeyValuePair<string, RelationalQueryPlan>>> Entries = new Dictionary<string, LinkedListNode<KeyValuePair<string, RelationalQueryPlan>>>();
        static readonly LinkedList<KeyValuePair<string, RelationalQueryPlan>> Recency = new LinkedList<KeyValuePair<string, RelationalQueryPlan>>();
        [ThreadStatic]
        static KeyWriter _keyWriter;
        internal static int Count { get { lock (Gate) return Entries.Count; } }

        internal static void Clear()
        {
            lock (Gate) { Entries.Clear(); Recency.Clear(); }
        }

        internal static RelationalQueryPlan GetOrAdd(string key, Func<RelationalQueryPlan> compile)
        {
            lock (Gate)
            {
                if (Entries.TryGetValue(key, out var entry))
                { Recency.Remove(entry); Recency.AddFirst(entry); return entry.Value.Value; }
                var plan = compile();
                var node = Recency.AddFirst(new KeyValuePair<string, RelationalQueryPlan>(key, plan));
                Entries.Add(key, node);
                if (Entries.Count > Capacity) { Entries.Remove(Recency.Last.Value.Key); Recency.RemoveLast(); }
                return plan;
            }
        }

        internal static string Key(Expression expression, SqlDialectKind dialect, Type resultType, string operation, bool async)
        {
            var writer = _keyWriter ?? new KeyWriter();
            _keyWriter = null;
            try
            {
                writer.Text.Append(dialect).Append('|').Append(resultType.TypeHandle.Value.ToInt64()).Append('|').Append(operation).Append('|').Append(async);
                writer.Visit(expression);
                return writer.Text.ToString();
            }
            finally
            {
                writer.Reset();
                if (writer.Text.Capacity <= 8192) _keyWriter = writer;
            }
        }

        private sealed class KeyWriter : ExpressionVisitor
        {
            internal readonly StringBuilder Text = new StringBuilder(512);
            readonly Dictionary<ParameterExpression, int> _parameters = new Dictionary<ParameterExpression, int>();
            internal void Reset() { Text.Clear(); _parameters.Clear(); }
            public override Expression Visit(Expression node)
            {
                if (node == null) { Text.Append("<null>"); return null; }
                Text.Append('(').Append((int)node.NodeType).Append(':').Append(node.Type.TypeHandle.Value.ToInt64()).Append(':');
                base.Visit(node);
                Text.Append(')');
                return node;
            }
            protected override Expression VisitExtension(Expression node)
            {
                if (node is SqlParameterExpression parameter) Text.Append("slot:").Append(parameter.Index);
                else if (node is QueryRootExpression root)
                {
                    Text.Append("table:"); Add(root.Mapping.TableName);
                    foreach (var column in root.Mapping.Columns)
                    {
                        Add(column.PropertyName); Add(column.Name); Text.Append(column.ColumnType.TypeHandle.Value.ToInt64()).Append(':');
                        Text.Append(column.IsNullable).Append(':').Append(column.StoreAsText).Append(':').Append(column.IsPK);
                    }
                }
                else throw new NotSupportedException("Unexpected normalized expression: " + node.GetType());
                return node;
            }
            void Add(string value) { Text.Append(value.Length).Append(':').Append(value); }
            void Member(MemberInfo member) { Text.Append(member.DeclaringType.TypeHandle.Value.ToInt64()).Append(':').Append(member.MetadataToken).Append(':'); }
            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (!_parameters.TryGetValue(node, out var index)) { index = _parameters.Count; _parameters.Add(node, index); }
                Text.Append(index); return node;
            }
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                Member(node.Method);
                foreach (var type in node.Method.GetGenericArguments()) Text.Append(type.TypeHandle.Value.ToInt64()).Append(':');
                return base.VisitMethodCall(node);
            }
            protected override Expression VisitMember(MemberExpression node) { Member(node.Member); return base.VisitMember(node); }
            protected override Expression VisitNew(NewExpression node)
            {
                if (node.Constructor != null) Member(node.Constructor);
                if (node.Members != null) foreach (var member in node.Members) Member(member);
                return base.VisitNew(node);
            }
            protected override MemberAssignment VisitMemberAssignment(MemberAssignment node) { Member(node.Member); return base.VisitMemberAssignment(node); }
            protected override Expression VisitBinary(BinaryExpression node) { if (node.Method != null) Member(node.Method); return base.VisitBinary(node); }
            protected override Expression VisitUnary(UnaryExpression node) { if (node.Method != null) Member(node.Method); return base.VisitUnary(node); }
        }
    }
}
