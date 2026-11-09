using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Kuery.Query;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    internal static class CompiledQueryCacheKeyGenerator
    {
        [ThreadStatic]
        static KeyWriter _keyWriter;
        internal static string GenerateCacheKey(Expression expression, SqlDialectKind dialect, Type resultType, string operation, bool async)
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
                if (node is QueryParameterExpression parameter) Text.Append("slot:").Append(parameter.Index);
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
