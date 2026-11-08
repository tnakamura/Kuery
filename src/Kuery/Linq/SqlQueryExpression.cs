using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kuery.Linq
{
    // SQL leaves participate in CLR projection expressions, but are never reducible to client code.
    internal abstract class SqlExpression : Expression
    {
        protected SqlExpression(Type type, bool nullable)
        {
            Type = type;
            IsNullable = nullable;
        }

        public sealed override ExpressionType NodeType => ExpressionType.Extension;
        public sealed override Type Type { get; }
        internal bool IsNullable { get; }
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }

    internal sealed class SqlColumnExpression : SqlExpression
    {
        internal SqlColumnExpression(string alias, string name, Type type, bool nullable)
            : base(type, nullable) { Alias = alias; Name = name; }
        internal string Alias { get; }
        internal string Name { get; }
    }

    internal sealed class SqlParameterExpression : SqlExpression
    {
        internal SqlParameterExpression(int index, Type type)
            : base(type, !type.IsValueType || Nullable.GetUnderlyingType(type) != null) { Index = index; }
        internal int Index { get; }
    }

    internal sealed class SqlConstantExpression : SqlExpression
    {
        internal SqlConstantExpression(object value, Type type) : base(type, value == null) { Value = value; }
        internal object Value { get; }
    }

    internal sealed class SqlOperationExpression : SqlExpression
    {
        internal SqlOperationExpression(string operation, Type type, bool nullable, params SqlExpression[] arguments)
            : base(type, nullable) { Operation = operation; Arguments = arguments; }
        internal string Operation { get; }
        internal IReadOnlyList<SqlExpression> Arguments { get; }
    }

    internal sealed class SqlSubqueryExpression : SqlExpression
    {
        internal SqlSubqueryExpression(string operation, SqlSelectExpression query, Type type, SqlExpression operand = null)
            : base(type, false) { Operation = operation; Query = query; Operand = operand; }
        internal string Operation { get; }
        internal SqlSelectExpression Query { get; }
        internal SqlExpression Operand { get; }
    }

    internal sealed class SqlTableSource
    {
        internal string TableName;
        internal string Alias;
        internal SqlSelectExpression Query;
        internal SqlSelectExpression Left;
        internal SqlSelectExpression Right;
        internal string SetOperator;
    }

    internal sealed class SqlJoinExpression
    {
        internal string Kind;
        internal SqlTableSource Source;
        internal SqlExpression Predicate;
    }

    internal sealed class SqlOrderingExpression
    {
        internal SqlOrderingExpression(SqlExpression expression, bool ascending)
        { Expression = expression; Ascending = ascending; }
        internal SqlExpression Expression;
        internal bool Ascending;
    }

    internal sealed class SqlSelectExpression
    {
        internal SqlTableSource From;
        internal readonly List<SqlJoinExpression> Joins = new List<SqlJoinExpression>();
        internal readonly List<SqlExpression> Projections = new List<SqlExpression>();
        internal readonly List<SqlExpression> GroupBy = new List<SqlExpression>();
        internal readonly List<SqlOrderingExpression> Orderings = new List<SqlOrderingExpression>();
        internal SqlExpression Predicate;
        internal SqlExpression Having;
        internal SqlExpression Offset;
        internal SqlExpression Limit;
        internal bool Distinct;
    }

    internal sealed class EntityProjectionExpression : Expression
    {
        internal EntityProjectionExpression(TableMapping mapping, IReadOnlyDictionary<string, Expression> members, SqlExpression presence = null)
        { Mapping = mapping; Members = members; Presence = presence; }
        internal TableMapping Mapping { get; }
        internal IReadOnlyDictionary<string, Expression> Members { get; }
        internal SqlExpression Presence { get; }
        public override Type Type => Mapping.MappedType;
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }

    internal sealed class GroupProjectionExpression : Expression
    {
        internal GroupProjectionExpression(Type type, Expression key, Expression element)
        { Type = type; Key = key; Element = element; }
        public override Type Type { get; }
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal Expression Key { get; }
        internal Expression Element { get; }
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }

    internal sealed class QueryRootExpression : Expression
    {
        internal QueryRootExpression(TableMapping mapping) { Mapping = mapping; }
        internal TableMapping Mapping { get; }
        public override Type Type => typeof(IQueryable<>).MakeGenericType(Mapping.MappedType);
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }

    internal enum QueryResultKind { Sequence, First, FirstOrDefault, Single, SingleOrDefault, ElementAt, ElementAtOrDefault, Scalar }

    internal sealed class RelationalQueryPlan
    {
        internal SqlSelectExpression Query;
        internal Expression Projection;
        internal QueryResultKind ResultKind;
        internal Type ElementType;
        internal Func<IDataRecord, object> Materialize;
        internal RelationalCommand CommandTemplate;
        internal string Mutation;
        internal TableMapping MutationTable;
        internal readonly List<KeyValuePair<string, SqlExpression>> Setters = new List<KeyValuePair<string, SqlExpression>>();
    }

    internal static class ProjectionReader
    {
        internal static Func<IDataRecord, object> Compile(SqlSelectExpression query, Expression projection)
        {
            var record = Expression.Parameter(typeof(IDataRecord), "record");
            var body = new ReaderVisitor(query, record).Visit(projection);
            return Expression.Lambda<Func<IDataRecord, object>>(Expression.Convert(body, typeof(object)), record).Compile();
        }

        internal static object ConvertValue(object value, Type type)
        {
            var target = Nullable.GetUnderlyingType(type) ?? type;
            if (value == null || value == DBNull.Value)
            {
                if (!type.IsValueType || Nullable.GetUnderlyingType(type) != null) return null;
                throw new InvalidOperationException("SQL NULL cannot be read as " + type + ". Project a nullable value instead.");
            }
            if (target.IsInstanceOfType(value)) return value;
            if (target.IsEnum) return value is string text ? Enum.Parse(target, text) : Enum.ToObject(target, value);
            if (target == typeof(Guid)) return value is byte[] bytes ? new Guid(bytes) : Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture));
            if (target == typeof(TimeSpan)) return value is long ticks ? new TimeSpan(ticks) : TimeSpan.Parse(Convert.ToString(value, CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
            if (target == typeof(DateTimeOffset)) return value is DateTime date ? new DateTimeOffset(date) : DateTimeOffset.Parse(Convert.ToString(value, CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
            if (target == typeof(DateTime)) return DateTime.Parse(Convert.ToString(value, CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
            return Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
        }

        private sealed class ReaderVisitor : ExpressionVisitor
        {
            readonly SqlSelectExpression _query;
            readonly ParameterExpression _record;
            internal ReaderVisitor(SqlSelectExpression query, ParameterExpression record) { _query = query; _record = record; }

            protected override Expression VisitExtension(Expression node)
            {
                if (node is SqlExpression sql)
                {
                    var ordinal = _query.Projections.Count;
                    _query.Projections.Add(sql);
                    return Expression.Convert(Expression.Call(typeof(ProjectionReader), nameof(ConvertValue), null,
                        Expression.Call(_record, typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetValue)), Expression.Constant(ordinal)),
                        Expression.Constant(sql.Type, typeof(Type))), sql.Type);
                }
                if (node is EntityProjectionExpression entity)
                {
                    var bindings = entity.Members.Select(pair => Expression.Bind(entity.Type.GetProperty(pair.Key), Visit(pair.Value)));
                    Expression result = Expression.MemberInit(Expression.New(entity.Type), bindings);
                    if (entity.Presence != null)
                    {
                        var presence = Visit(entity.Presence);
                        result = Expression.Condition(Expression.Equal(presence, Expression.Constant(null, presence.Type)), Expression.Default(entity.Type), result);
                    }
                    return result;
                }
                throw new NotSupportedException("An unprojected GroupBy is not supported. Select its key and aggregates.");
            }
        }
    }
}
