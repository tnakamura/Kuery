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
            if (node is OptionalShaperExpression optional)
                return Expression.Condition(Expression.Equal(Visit(optional.Presence), Expression.Constant(null, optional.Presence.Type)),
                    Expression.Default(optional.Type), Visit(optional.Value));
            if (node is SqlExpression sql)
            {
                var ordinal = _query.AddToProjection(sql);
                return Expression.Convert(Expression.Call(typeof(RelationalShapedQueryCompilingExpressionVisitor), nameof(ConvertValue), null,
                    Expression.Call(_record, typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetValue)), Expression.Constant(ordinal)),
                    Expression.Constant(sql.Type, typeof(Type))), sql.Type);
            }
            if (node is StructuralTypeProjectionExpression projection) return Visit(new StructuralTypeShaperExpression(projection));
            if (node is StructuralTypeShaperExpression shaper)
            {
                var entity = shaper.Projection;
                Expression Construct(TableMapping mapping) => Expression.Convert(Expression.MemberInit(Expression.New(mapping.MappedType),
                    mapping.Columns.Select(column => Expression.Bind(column.PropertyInfo, Visit(entity.Members[column.PropertyName])))), entity.Type);
                Expression result;
                if (entity.Hierarchy == null) result = Construct(entity.Mapping);
                else
                {
                    var discriminator = Visit(entity.Discriminator);
                    result = Expression.Throw(Expression.New(typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) }),
                        Expression.Constant("Unknown discriminator value for TPH hierarchy " + entity.Hierarchy.Root.MappedType + ".")), entity.Type);
                    foreach (var entry in entity.ConcreteTypes.Reverse())
                        result = Expression.Condition(Expression.Equal(discriminator, Expression.Constant(entry.Value)), Construct(entry.Mapping), result);
                }
                if (entity.Presence != null)
                    result = Expression.Condition(Expression.Equal(Visit(entity.Presence), Expression.Constant(null, entity.Presence.Type)), Expression.Default(entity.Type), result);
                return result;
            }
            throw new NotSupportedException("An unprojected GroupBy is not supported. Select its key and aggregates.");
        }
    }
}
