using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Compiles expressions that construct CLR results from projected SQL rows.</summary>
    internal sealed class RelationalShapedQueryCompilingExpressionVisitor : ExpressionVisitor
    {
        readonly ParameterExpression _record = Expression.Parameter(typeof(IDataRecord), "record");
        SelectExpression _query;

        /// <summary>Compiles the delegates needed to construct query results from SQL rows.</summary>
        /// <param name="shapedQuery">The translated SELECT and the associated CLR projection.</param>
        /// <returns>The SQL tree and its compiled result delegates.</returns>
        internal CompiledQuery Compile(ShapedQueryExpression shapedQuery)
        {
            _query = shapedQuery.QueryExpression;
            if (shapedQuery.Collection != null)
                return new CompiledQuery { ShapedQuery = shapedQuery, Collection = CompileCollection(shapedQuery) };

            return new CompiledQuery
            {
                ShapedQuery = shapedQuery,
                Materialize = ReadValue<object>(shapedQuery.ShaperExpression)
            };
        }

        Func<IDataRecord, T> ReadValue<T>(Expression expression)
            => Expression.Lambda<Func<IDataRecord, T>>(Expression.Convert(Visit(expression), typeof(T)), _record).Compile();

        CollectionQueryPlan CompileCollection(ShapedQueryExpression query)
        {
            var collection = query.Collection;
            var binding = new CollectionProjectionBindingVisitor(collection);
            var result = binding.Visit(query.ShaperExpression);
            var values = Expression.NewArrayInit(typeof(object), binding.OuterValues.Select(e => Expression.Convert(Visit(e), typeof(object))));
            var hasElement = collection.Presence == null ? (Expression)Expression.Constant(true)
                : Expression.NotEqual(Visit(collection.Presence), Expression.Constant(null, collection.Presence.Type));

            return new CollectionQueryPlan
            {
                ReadIdentifier = ReadValue<long>(collection.Identifier),
                ReadElement = ReadValue<object>(collection.Element),
                HasElement = Expression.Lambda<Func<IDataRecord, bool>>(hasElement, _record).Compile(),
                ReadOuterValues = Expression.Lambda<Func<IDataRecord, object[]>>(values, _record).Compile(),
                CreateResult = Expression.Lambda<Func<object[], IList, object>>(Expression.Convert(result, typeof(object)), binding.Values, binding.Elements).Compile(),
                ListType = typeof(List<>).MakeGenericType(collection.Element.Type)
            };
        }

        sealed class CollectionProjectionBindingVisitor : ExpressionVisitor
        {
            readonly CollectionShaperExpression _collection;
            /// <summary>The expressions to evaluate once from the first row of a group.</summary>
            internal readonly List<Expression> OuterValues = new List<Expression>();
            /// <summary>The lambda parameter holding the group's shared values.</summary>
            internal readonly ParameterExpression Values = Expression.Parameter(typeof(object[]), "values");
            /// <summary>The lambda parameter holding the accumulated elements of the group.</summary>
            internal readonly ParameterExpression Elements = Expression.Parameter(typeof(IList), "elements");

            /// <summary>Creates a binder for the collection being constructed.</summary>
            /// <param name="collection">The collection shaper being bound.</param>
            internal CollectionProjectionBindingVisitor(CollectionShaperExpression collection)
            {
                _collection = collection;
            }

            /// <summary>Binds the collection and outer values used to construct a grouped result.</summary>
            /// <param name="node">The expression to visit.</param>
            /// <returns>The visited expression.</returns>
            public override Expression Visit(Expression node)
            {
                if (node == null) return null;
                if (ReferenceEquals(node, _collection)) return Expression.Convert(Elements, node.Type);
                var finder = new CollectionFindingVisitor();
                finder.Visit(node);
                if (finder.Found) return base.Visit(node);

                var index = OuterValues.Count;
                OuterValues.Add(node);
                return Expression.Convert(Expression.ArrayIndex(Values, Expression.Constant(index)), node.Type);
            }

            /// <summary>Rebinds the constructor arguments and assignments of a grouped result initializer.</summary>
            /// <param name="node">The object initializer to visit.</param>
            /// <returns>The visited expression.</returns>
            protected override Expression VisitMemberInit(MemberInitExpression node)
                => node.Update(node.NewExpression.Update(node.NewExpression.Arguments.Select(Visit)), Visit(node.Bindings, VisitMemberBinding));
        }

        sealed class CollectionFindingVisitor : ExpressionVisitor
        {
            /// <summary>Whether a collection projection was found.</summary>
            internal bool Found;
            /// <summary>Detects a collection shaper in a projected result.</summary>
            /// <param name="node">The expression to visit.</param>
            /// <returns>The visited expression.</returns>
            protected override Expression VisitExtension(Expression node)
            {
                if (node is CollectionShaperExpression) Found = true;
                return node;
            }
        }

        /// <summary>Converts a provider value to the CLR type required by a projection.</summary>
        /// <param name="value">The reader value to convert, including DBNull.</param>
        /// <param name="type">The target CLR type of the projection.</param>
        /// <returns>The converted CLR value or null for a nullable target.</returns>
        /// <exception cref="System.InvalidOperationException">A SQL NULL was read for a non-nullable target type.</exception>
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

        /// <summary>Builds expressions that read SQL columns and construct the corresponding CLR values.</summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The expression used to materialize the value.</returns>
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
