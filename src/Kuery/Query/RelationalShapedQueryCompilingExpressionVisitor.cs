using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class RelationalShapedQueryCompilingExpressionVisitor : ExpressionVisitor
    {
        readonly ParameterExpression _record = Expression.Parameter(typeof(IDataRecord), "record");
        SelectExpression _query;

        internal CompiledQuery Compile(ShapedQueryExpression shapedQuery)
        {
            _query = shapedQuery.QueryExpression;
            var record = _record;
            var body = Visit(shapedQuery.ShaperExpression);
            return new CompiledQuery { ShapedQuery = shapedQuery, Materialize = Expression.Lambda<Func<IDataRecord, object>>(Expression.Convert(body, typeof(object)), record).Compile() };
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

        protected override Expression VisitExtension(Expression node)
        {
            if (node is SqlExpression sql)
            {
                var ordinal = _query.AddToProjection(sql);
                return Expression.Convert(Expression.Call(typeof(RelationalShapedQueryCompilingExpressionVisitor), nameof(ConvertValue), null,
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
