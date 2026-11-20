using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class RelationalQueryableMethodTranslatingExpressionVisitor : ExpressionVisitor
    {
        int _alias;
        internal string Alias() => "t" + _alias++;
        internal static SqlConstantExpression Integer(int value) => new SqlConstantExpression(value, typeof(int));
        readonly SqlExpressionFactory _sqlExpressionFactory = new SqlExpressionFactory();
        readonly RelationalSqlTranslatingExpressionVisitor _sqlTranslator;

        internal RelationalQueryableMethodTranslatingExpressionVisitor()
        {
            _sqlTranslator = new RelationalSqlTranslatingExpressionVisitor(this);
        }

        internal static SqlExpression And(SqlExpression left, SqlExpression right)
            => left == null ? right : new SqlBinaryExpression(ExpressionType.AndAlso, left, right, typeof(bool), false);

        internal NonQueryExpression TranslateExecuteDelete(Expression expression) => TranslateMutation(expression, "delete", null);

        internal NonQueryExpression TranslateExecuteUpdate(Expression expression, IReadOnlyList<SetPropertyCall> setters)
            => TranslateMutation(expression, "update", setters);

        NonQueryExpression TranslateMutation(Expression expression, string operation, IReadOnlyList<SetPropertyCall> setters)
        {
            // Validate the syntax as well as the resulting SQL shape: ignored operators must not broaden a mutation.
            var root = expression;
            while (root is MethodCallExpression call && call.Method.DeclaringType == typeof(Queryable) && call.Method.Name == nameof(Queryable.Where))
                root = call.Arguments[0];
            if (root is QueryRootExpression mutationRoot && InheritanceMapping.Find(mutationRoot.Mapping.MappedType) != null)
                throw new NotSupportedException("TPH mapping is query-only; bulk mutations are not supported.");
            if (!(root is QueryRootExpression)) throw new NotSupportedException(operation + " supports only a single table with Where conditions.");
            var source = Translate(expression);
            if (source.QueryExpression.Predicate == null) throw new InvalidOperationException("No condition specified");
            var nonQuery = new NonQueryExpression { Operation = operation, QueryExpression = source.QueryExpression, Table = ((StructuralTypeProjectionExpression)source.ShaperExpression).Mapping };
            if (operation == "update")
            {
                if (setters == null || setters.Count == 0) throw new InvalidOperationException("No setters specified");
                var names = new HashSet<string>();
                foreach (var setter in setters)
                {
                    var property = StripConvert(setter.PropertyExpression.Body) as MemberExpression;
                    if (property == null || property.Expression != setter.PropertyExpression.Parameters[0])
                        throw new NotSupportedException("SetProperty requires a direct mapped property access.");
                    var column = nonQuery.Table.FindColumnWithPropertyName(property.Member.Name);
                    if (column == null || !names.Add(column.Name)) throw new NotSupportedException("Unknown or duplicate update property: " + property.Member.Name);
                    nonQuery.Setters.Add(new KeyValuePair<string, SqlExpression>(column.Name, Scalar(Bind(setter.ValueExpression, source.ShaperExpression))));
                }
            }
            return nonQuery;
        }

        internal ShapedQueryExpression Translate(Expression expression) => (ShapedQueryExpression)Visit(expression);

        protected override Expression VisitExtension(Expression expression)
        {
            if (!(expression is QueryRootExpression root)) throw Unsupported(expression);
            var alias = Alias();
            var hierarchy = InheritanceMapping.Find(root.Mapping.MappedType);
            var members = (hierarchy?.Columns ?? root.Mapping.Columns).ToDictionary(c => c.PropertyName, c =>
            {
                var type = c.PropertyInfo.PropertyType;
                return (Expression)new ColumnExpression(alias, c.Name, type, c.IsNullable && (!type.IsValueType || Nullable.GetUnderlyingType(type) != null));
            });
            var discriminator = hierarchy == null ? null : new ColumnExpression(alias, hierarchy.DiscriminatorColumn, typeof(string), true);
            var projection = new StructuralTypeProjectionExpression(root.Mapping, members, hierarchy: hierarchy, discriminator: discriminator);
            var query = new ShapedQueryExpression
            {
                QueryExpression = new SelectExpression { From = new TableExpression(hierarchy?.Root.TableName ?? root.Mapping.TableName, alias) },
                ShaperExpression = projection,
            };
            if (hierarchy != null && root.Mapping.MappedType != hierarchy.Root.MappedType)
                query.QueryExpression.Predicate = TypePredicate(projection);
            return query;
        }

        protected override Expression VisitMethodCall(MethodCallExpression method)
        {
            if (method.Method.DeclaringType != typeof(Queryable) && method.Method.DeclaringType != typeof(KueryQueryableExtensions)) throw Unsupported(method);
            var name = method.Method.Name;
            if (name == nameof(Queryable.SelectMany) && method.Arguments[0] is MethodCallExpression groupJoin
                && groupJoin.Method.DeclaringType == typeof(Queryable) && groupJoin.Method.Name == nameof(Queryable.GroupJoin))
                return TranslateLeftJoin(groupJoin, method);
            if (name == nameof(Queryable.Join)) return TranslateJoin(method);
            if (name == "LeftJoin") return TranslateLeftJoin(method);
            if (name == "RightJoin") return TranslateRightJoin(method);
            if (name == "FullJoin") return TranslateFullJoin(method);
            if (name == nameof(Queryable.Union) || name == nameof(Queryable.Concat) || name == nameof(Queryable.Intersect) || name == nameof(Queryable.Except))
                return TranslateSetOperation(method);
            var source = Translate(method.Arguments[0]);
            switch (name)
            {
                case nameof(Queryable.OfType): return TranslateOfType(source, method.Method.GetGenericArguments()[0]);
                case nameof(Queryable.Reverse): return TranslateReverse(source);
                case nameof(Queryable.Cast): return TranslateCast(source, method.Method.GetGenericArguments()[0]);
                case nameof(Queryable.Contains):
                    if (method.Arguments.Count != 2) throw Unsupported(method);
                    return TranslateContains(source, method.Arguments[1]);
                case nameof(Queryable.DefaultIfEmpty):
                    if (method.Arguments.Count != 1) throw Unsupported(method);
                    return TranslateDefaultIfEmpty(source);
                case nameof(Queryable.Where):
                    TranslateWhere(source, Lambda(method.Arguments[1]));
                    break;
                case nameof(Queryable.Select):
                    return TranslateSelect(source, method);
                case nameof(Queryable.OrderBy):
                case nameof(Queryable.OrderByDescending):
                    if (method.Arguments.Count != 2) throw Unsupported(method);
                    return TranslateOrderBy(source, Lambda(method.Arguments[1]), name == nameof(Queryable.OrderBy));
                case nameof(Queryable.ThenBy):
                case nameof(Queryable.ThenByDescending):
                    if (method.Arguments.Count != 2) throw Unsupported(method);
                    return TranslateThenBy(source, Lambda(method.Arguments[1]), name == nameof(Queryable.ThenBy));
                case nameof(Queryable.Skip):
                    return TranslateSkip(source, method);
                case nameof(Queryable.Take):
                    return TranslateTake(source, method);
                case nameof(Queryable.Distinct):
                    return TranslateDistinct(source, method);
                case nameof(Queryable.GroupBy):
                    return TranslateGroupBy(source, method);
                case nameof(Queryable.SelectMany):
                    return TranslateSelectMany(source, method);
                case nameof(Queryable.Count):
                case nameof(Queryable.LongCount):
                case nameof(Queryable.Sum):
                case nameof(Queryable.Min):
                case nameof(Queryable.Max):
                case nameof(Queryable.Average):
                    return TranslateAggregateWithSelector(source, method);
                case nameof(Queryable.Any):
                    return TranslateAny(source, method.Arguments.Count == 2 ? Lambda(method.Arguments[1]) : null);
                case nameof(Queryable.All):
                    return TranslateAll(source, Lambda(method.Arguments[1]));
                case nameof(Queryable.First):
                case nameof(Queryable.FirstOrDefault):
                case nameof(Queryable.Single):
                case nameof(Queryable.SingleOrDefault):
                case nameof(Queryable.Last):
                case nameof(Queryable.LastOrDefault):
                case nameof(Queryable.ElementAt):
                case nameof(Queryable.ElementAtOrDefault):
                    return TranslateSingleResult(source, method);
                default: throw Unsupported(method);
            }
            return source;
        }

        ShapedQueryExpression TranslateOfType(ShapedQueryExpression source, Type resultType)
        {
            if (source.QueryExpression.Limit != null || source.QueryExpression.Offset != null || source.QueryExpression.IsDistinct)
                PushdownIntoSubquery(source);
            if (!(StripConvert(source.ShaperExpression) is StructuralTypeProjectionExpression entity)) throw new NotSupportedException("OfType requires a mapped entity hierarchy.");
            if (entity.Hierarchy == null)
            {
                if (!resultType.IsAssignableFrom(entity.Type)) throw new NotSupportedException("OfType requires an explicitly registered TPH hierarchy: " + resultType);
                source.QueryExpression.Predicate = And(source.QueryExpression.Predicate, IsPresent(entity.Presence));
                source.ShaperExpression = resultType == entity.Type ? (Expression)entity : Expression.Convert(entity, resultType);
                return source;
            }
            if (!entity.Hierarchy.Root.MappedType.IsAssignableFrom(resultType) && !resultType.IsAssignableFrom(entity.Hierarchy.Root.MappedType))
                throw new NotSupportedException("OfType target is not in the mapped hierarchy: " + resultType);
            if (resultType.IsInterface || resultType == typeof(object))
            {
                source.QueryExpression.Predicate = And(source.QueryExpression.Predicate, IsPresent(entity.Presence));
                source.ShaperExpression = Expression.Convert(entity, resultType);
                return source;
            }
            if (resultType != entity.Hierarchy.Root.MappedType && !entity.Hierarchy.Entries.Any(e => resultType.IsAssignableFrom(e.Mapping.MappedType)))
                throw new NotSupportedException("OfType target is not registered in the hierarchy: " + resultType);
            var typed = entity.WithType(resultType);
            source.QueryExpression.Predicate = And(source.QueryExpression.Predicate, And(IsPresent(entity.Presence), TypePredicate(typed)));
            source.ShaperExpression = typed;
            return source;
        }

        SqlExpression TypePredicate(StructuralTypeProjectionExpression entity)
        {
            SqlExpression predicate = new SqlConstantExpression(false, typeof(bool));
            foreach (var entry in entity.ConcreteTypes)
                predicate = _sqlExpressionFactory.MakeBinary(ExpressionType.OrElse, typeof(bool), predicate,
                    _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), entity.Discriminator, new SqlConstantExpression(entry.Value, typeof(string))));
            return predicate;
        }

        ShapedQueryExpression TranslateReverse(ShapedQueryExpression source)
        {
            if (source.QueryExpression.Orderings.Count == 0) throw new NotSupportedException("Reverse requires an explicit OrderBy.");
            if (source.QueryExpression.Limit != null || source.QueryExpression.Offset != null) PushdownIntoSubquery(source);
            foreach (var ordering in source.QueryExpression.Orderings) ordering.IsAscending = !ordering.IsAscending;
            return source;
        }

        ShapedQueryExpression TranslateCast(ShapedQueryExpression source, Type resultType)
        {
            if (source.ShaperExpression.Type != resultType)
            {
                try { source.ShaperExpression = Expression.Convert(source.ShaperExpression, resultType); }
                catch (InvalidOperationException ex) { throw new NotSupportedException("Cannot cast query result to " + resultType, ex); }
            }
            return source;
        }

        internal ShapedQueryExpression TranslateContains(ShapedQueryExpression source, Expression item)
        {
            PushdownIntoSubquery(source, false);
            var shape = StripConvert(source.ShaperExpression);
            SqlExpression predicate;
            if (shape is StructuralTypeProjectionExpression entity)
            {
                if (entity.Mapping.Columns.Count(c => c.IsPK) != 1) throw new NotSupportedException("Entity Contains requires a single mapped primary key.");
                var key = Scalar(entity.Members[entity.Mapping.PK.PropertyName]);
                var other = StripConvert(item);
                SqlExpression otherKey;
                SqlExpression present;
                if (other is EntityReferenceExpression reference)
                { otherKey = Scalar(reference.Key); present = Scalar(reference.Present); }
                else if (other is StructuralTypeProjectionExpression otherEntity && otherEntity.Mapping.Columns.Count(c => c.IsPK) == 1)
                { otherKey = Scalar(otherEntity.Members[otherEntity.Mapping.PK.PropertyName]); present = IsPresent(otherEntity.Presence); }
                else throw Unsupported(item);
                predicate = And(And(IsPresent(entity.Presence), present), _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), key, otherKey));
                if (entity.Presence != null)
                    predicate = _sqlExpressionFactory.MakeBinary(ExpressionType.OrElse, typeof(bool), predicate,
                        And(_sqlExpressionFactory.MakeUnary(ExpressionType.Equal, typeof(bool), entity.Presence),
                            _sqlExpressionFactory.MakeUnary(ExpressionType.Not, typeof(bool), present)));
            }
            else predicate = _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), Scalar(source.ShaperExpression), Scalar(item));
            source.QueryExpression.Predicate = And(source.QueryExpression.Predicate, predicate);
            return CreateExistsResult(source, false);
        }

        SqlExpression IsPresent(SqlExpression presence) => presence == null ? (SqlExpression)new SqlConstantExpression(true, typeof(bool))
            : _sqlExpressionFactory.MakeUnary(ExpressionType.NotEqual, typeof(bool), presence);

        ShapedQueryExpression TranslateDefaultIfEmpty(ShapedQueryExpression source)
        {
            MakeOptional(source, true);
            var singleton = new SelectExpression(Alias());
            singleton.AddToProjection(Integer(1));
            var outer = new SelectExpression { From = singleton };
            outer.Joins.Add(new LeftJoinExpression(source.QueryExpression.From, new SqlConstantExpression(true, typeof(bool))));
            outer.Orderings.AddRange(source.QueryExpression.Orderings);
            source.QueryExpression = outer;
            return source;
        }

        SqlExpression MakeOptional(ShapedQueryExpression source, bool liftOrderings = false)
        {
            var inner = source.QueryExpression;
            var alias = Alias();
            var lift = new ProjectionLifter(inner, alias);
            var value = new NullableProjectionVisitor().Visit(lift.Visit(source.ShaperExpression));
            var marker = (SqlExpression)lift.Visit(new SqlConstantExpression(1, typeof(int?)));
            inner.Alias = alias;
            source.QueryExpression = new SelectExpression { From = inner };
            if (liftOrderings && !inner.IsDistinct)
                foreach (var ordering in inner.Orderings)
                    source.QueryExpression.Orderings.Add(new OrderingExpression((SqlExpression)lift.Visit(ordering.Expression), ordering.IsAscending));
            if (inner.Limit == null && inner.Offset == null) inner.Orderings.Clear();
            source.ShaperExpression = value is StructuralTypeProjectionExpression entity
                ? (Expression)entity.Update(entity.Members, entity.Presence ?? marker, entity.Discriminator)
                : new OptionalShaperExpression(value, marker);
            return marker;
        }

        ShapedQueryExpression TranslateSelect(ShapedQueryExpression source, MethodCallExpression method)
        {
            var query = source.QueryExpression;
            if (query.IsDistinct) PushdownIntoSubquery(source);
            source.ShaperExpression = Projection(Bind(Lambda(method.Arguments[1]), source.ShaperExpression));
            return source;
        }

        ShapedQueryExpression TranslateOrderBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
        {
            if (source.QueryExpression.Limit != null || source.QueryExpression.Offset != null || source.QueryExpression.IsDistinct)
                PushdownIntoSubquery(source);
            source.QueryExpression.Orderings.Clear();
            source.QueryExpression.Orderings.Add(new OrderingExpression(Scalar(Bind(keySelector, source.ShaperExpression)), ascending));
            return source;
        }

        ShapedQueryExpression TranslateThenBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
        {
            if (source.QueryExpression.Limit != null || source.QueryExpression.Offset != null || source.QueryExpression.IsDistinct)
                PushdownIntoSubquery(source);
            source.QueryExpression.Orderings.Add(new OrderingExpression(Scalar(Bind(keySelector, source.ShaperExpression)), ascending));
            return source;
        }

        ShapedQueryExpression TranslateSkip(ShapedQueryExpression source, MethodCallExpression method)
        {
            var query = source.QueryExpression;
            if (method.Arguments[1].Type != typeof(int)) throw Unsupported(method);
            // A subquery avoids arithmetic overflow and preserves Take().Skip() exactly.
            if (query.Limit != null || query.Offset != null) PushdownIntoSubquery(source);
            source.QueryExpression.Offset = _sqlExpressionFactory.Function("NonNegative", typeof(int), Scalar(method.Arguments[1]));
            return source;
        }

        ShapedQueryExpression TranslateTake(ShapedQueryExpression source, MethodCallExpression method)
        {
            var query = source.QueryExpression;
            if (method.Arguments[1].Type != typeof(int)) throw Unsupported(method);
            var take = _sqlExpressionFactory.Function("NonNegative", typeof(int), Scalar(method.Arguments[1]));
            query.Limit = query.Limit == null ? take : _sqlExpressionFactory.Function("Least", typeof(int), query.Limit, take);
            return source;
        }

        ShapedQueryExpression TranslateDistinct(ShapedQueryExpression source, MethodCallExpression method)
        {
            var query = source.QueryExpression;
            if (method.Arguments.Count != 1) throw Unsupported(method);
            if (query.Limit != null || query.Offset != null) PushdownIntoSubquery(source);
            source.ShaperExpression = Projection(source.ShaperExpression);
            source.QueryExpression.IsDistinct = true;
            source.QueryExpression.Orderings.Clear();
            return source;
        }

        ShapedQueryExpression TranslateGroupBy(ShapedQueryExpression source, MethodCallExpression method)
        {
            var query = source.QueryExpression;
            if (method.Arguments.Count != 2) throw Unsupported(method);
            if (query.Limit != null || query.Offset != null || query.IsDistinct || query.GroupBy.Count > 0) PushdownIntoSubquery(source);
            var key = Projection(Bind(Lambda(method.Arguments[1]), source.ShaperExpression));
            source.QueryExpression.GroupBy.AddRange(Leaves(key));
            source.QueryExpression.Orderings.Clear();
            source.ShaperExpression = new GroupByShaperExpression(method.Type.GetGenericArguments()[0], key, source.ShaperExpression);
            return source;
        }

        ShapedQueryExpression TranslateSelectMany(ShapedQueryExpression source, MethodCallExpression method)
        {
            var query = source.QueryExpression;
            var collection = Bind(Lambda(method.Arguments[1]), source.ShaperExpression);
            var correlation = new CorrelationDetector();
            correlation.Visit(collection);
            if (correlation.Found) throw new NotSupportedException("Correlated SelectMany requires lateral joins. Use a correlated Any/Contains subquery or an explicit Join.");
            var inner = Translate(collection);
            PushdownIntoSubquery(source);
            PushdownIntoSubquery(inner, false);
            source.QueryExpression.Joins.Add(new CrossJoinExpression(inner.QueryExpression.From));
            source.ShaperExpression = method.Arguments.Count == 3
                ? Projection(Bind(Lambda(method.Arguments[2]), source.ShaperExpression, inner.ShaperExpression)) : inner.ShaperExpression;
            return source;
        }

        ShapedQueryExpression TranslateAggregateWithSelector(ShapedQueryExpression source, MethodCallExpression method)
        {
            var query = source.QueryExpression;
            var name = method.Method.Name;
            if ((name == nameof(Queryable.Count) || name == nameof(Queryable.LongCount)) && method.Arguments.Count == 2)
                TranslateWhere(source, Lambda(method.Arguments[1]));
            if ((name == nameof(Queryable.Count) || name == nameof(Queryable.LongCount)) && source.ShaperExpression is GroupByShaperExpression countGroup)
                source.ShaperExpression = countGroup.Key;
            if (source.QueryExpression.IsDistinct || source.QueryExpression.Limit != null || source.QueryExpression.Offset != null || source.QueryExpression.GroupBy.Count > 0) PushdownIntoSubquery(source);
            SqlExpression argument = null;
            if (name != nameof(Queryable.Count) && name != nameof(Queryable.LongCount))
                argument = Scalar(method.Arguments.Count == 2 ? Bind(Lambda(method.Arguments[1]), source.ShaperExpression) : source.ShaperExpression);
            source.QueryExpression.Orderings.Clear();
            var aggregate = new SqlFunctionExpression(name, method.Type, true, argument == null ? Array.Empty<SqlExpression>() : new[] { argument });
            source.ShaperExpression = name == nameof(Queryable.Sum)
                ? _sqlExpressionFactory.Function("Coalesce", method.Type, aggregate, new SqlConstantExpression(Activator.CreateInstance(Nullable.GetUnderlyingType(method.Type) ?? method.Type), method.Type)) : aggregate;
            source.ResultKind = QueryResultKind.Scalar;
            return source;
        }

        ShapedQueryExpression TranslateAny(ShapedQueryExpression source, LambdaExpression predicate)
        {
            if (predicate != null) TranslateWhere(source, predicate);
            return CreateExistsResult(source, false);
        }

        ShapedQueryExpression TranslateAll(ShapedQueryExpression source, LambdaExpression predicate)
        {
            var query = source.QueryExpression;
            if (query.Limit != null || query.Offset != null || query.IsDistinct) PushdownIntoSubquery(source);
            var negated = _sqlExpressionFactory.MakeUnary(ExpressionType.Not, typeof(bool), Scalar(Bind(predicate, source.ShaperExpression)));
            if (source.ShaperExpression is GroupByShaperExpression) source.QueryExpression.Having = And(source.QueryExpression.Having, negated);
            else if (source.QueryExpression.GroupBy.Count > 0)
            {
                PushdownIntoSubquery(source);
                source.QueryExpression.Predicate = _sqlExpressionFactory.MakeUnary(ExpressionType.Not, typeof(bool), Scalar(Bind(predicate, source.ShaperExpression)));
            }
            else source.QueryExpression.Predicate = And(source.QueryExpression.Predicate, negated);
            return CreateExistsResult(source, true);
        }

        ShapedQueryExpression CreateExistsResult(ShapedQueryExpression source, bool negated)
        {
            if (source.ShaperExpression is GroupByShaperExpression group) source.ShaperExpression = group.Key;
            if (source.QueryExpression.GroupBy.Count > 0 || source.QueryExpression.IsDistinct) PushdownIntoSubquery(source);
            source.QueryExpression.AddToProjection(Integer(1));
            if (source.QueryExpression.Limit == null && source.QueryExpression.Offset == null) source.QueryExpression.Orderings.Clear();
            var exists = new ExistsExpression(source.QueryExpression);
            source.ShaperExpression = negated ? (SqlExpression)_sqlExpressionFactory.MakeUnary(ExpressionType.Not, typeof(bool), exists) : exists;
            source.QueryExpression = new SelectExpression();
            source.ResultKind = QueryResultKind.Scalar;
            return source;
        }

        ShapedQueryExpression TranslateSingleResult(ShapedQueryExpression source, MethodCallExpression method)
        {
            var query = source.QueryExpression;
            var name = method.Method.Name;
            if (name.StartsWith("ElementAt", StringComparison.Ordinal))
            {
                if (method.Arguments[1].Type != typeof(int)) throw Unsupported(method);
                if (query.Limit != null || query.Offset != null) PushdownIntoSubquery(source);
                // Negative indices produce no rows; the result cardinality determines throw/default.
                var index = Scalar(method.Arguments[1]);
                source.QueryExpression.Predicate = And(source.QueryExpression.Predicate, _sqlExpressionFactory.MakeBinary(ExpressionType.GreaterThanOrEqual, typeof(bool), index, Integer(0)));
                source.QueryExpression.Offset = _sqlExpressionFactory.Function("NonNegative", typeof(int), index);
            }
            else if (method.Arguments.Count == 2)
            {
                if (StripQuote(method.Arguments[1]) is LambdaExpression predicate) TranslateWhere(source, predicate);
                else throw Unsupported(method);
            }
            if (name.StartsWith("Last", StringComparison.Ordinal))
            {
                if (source.QueryExpression.Orderings.Count == 0) throw new NotSupportedException("Last requires an explicit OrderBy.");
                if (source.QueryExpression.Limit != null || source.QueryExpression.Offset != null) PushdownIntoSubquery(source);
                foreach (var ordering in source.QueryExpression.Orderings) ordering.IsAscending = !ordering.IsAscending;
            }
            bool single = name.StartsWith("Single", StringComparison.Ordinal);
            var limit = Integer(single ? 2 : 1);
            source.QueryExpression.Limit = source.QueryExpression.Limit == null ? (SqlExpression)limit : _sqlExpressionFactory.Function("Least", typeof(int), source.QueryExpression.Limit, limit);
            source.ResultKind = single
                ? (name.EndsWith("OrDefault", StringComparison.Ordinal) ? QueryResultKind.SingleOrDefault : QueryResultKind.Single)
                : (name.EndsWith("OrDefault", StringComparison.Ordinal) ? QueryResultKind.FirstOrDefault : QueryResultKind.First);
            if (name == nameof(Queryable.ElementAt)) source.ResultKind = QueryResultKind.ElementAt;
            if (name == nameof(Queryable.ElementAtOrDefault)) source.ResultKind = QueryResultKind.ElementAtOrDefault;
            return source;
        }

        ShapedQueryExpression TranslateWhere(ShapedQueryExpression source, LambdaExpression predicate)
        {
            if (source.QueryExpression.Limit != null || source.QueryExpression.Offset != null || source.QueryExpression.IsDistinct) PushdownIntoSubquery(source);
            var sql = Scalar(Bind(predicate, source.ShaperExpression));
            if (source.ShaperExpression is GroupByShaperExpression) source.QueryExpression.Having = And(source.QueryExpression.Having, sql);
            else if (source.QueryExpression.GroupBy.Count > 0) { PushdownIntoSubquery(source); source.QueryExpression.Predicate = Scalar(Bind(predicate, source.ShaperExpression)); }
            else source.QueryExpression.Predicate = And(source.QueryExpression.Predicate, sql);
            return source;
        }

        ShapedQueryExpression TranslateJoin(MethodCallExpression method)
        {
            if (method.Arguments.Count != 5) throw Unsupported(method);
            var outer = Translate(method.Arguments[0]);
            var inner = Translate(method.Arguments[1]);
            PushdownIntoSubquery(outer);
            PushdownIntoSubquery(inner, false);
            var predicate = JoinKeys(Bind(Lambda(method.Arguments[2]), outer.ShaperExpression), Bind(Lambda(method.Arguments[3]), inner.ShaperExpression));
            outer.QueryExpression.Joins.Add(new InnerJoinExpression(inner.QueryExpression.From, predicate));
            outer.ShaperExpression = Projection(Bind(Lambda(method.Arguments[4]), outer.ShaperExpression, inner.ShaperExpression));
            return outer;
        }

        ShapedQueryExpression TranslateLeftJoin(MethodCallExpression method) => TranslateOuterJoin(method, false, false);
        ShapedQueryExpression TranslateRightJoin(MethodCallExpression method) => TranslateOuterJoin(method, true, false);
        ShapedQueryExpression TranslateFullJoin(MethodCallExpression method) => TranslateOuterJoin(method, false, true);

        ShapedQueryExpression TranslateOuterJoin(MethodCallExpression method, bool reverse, bool full)
        {
            if (method.Arguments.Count != 5 && !(full && method.Arguments.Count == 6
                && method.Arguments[5] is ConstantExpression comparer && comparer.Value == null)) throw Unsupported(method);
            var left = Translate(method.Arguments[0]);
            var right = Translate(method.Arguments[1]);
            var outer = reverse ? right : left;
            var inner = reverse ? left : right;
            SqlExpression outerPresence = null;
            if (full) outerPresence = MakeOptional(outer);
            else PushdownIntoSubquery(outer);
            MakeOptional(inner);
            var predicate = JoinKeys(Bind(Lambda(method.Arguments[2]), left.ShaperExpression), Bind(Lambda(method.Arguments[3]), right.ShaperExpression));
            outer.QueryExpression.Joins.Add(full
                ? (JoinExpressionBase)new FullJoinExpression(inner.QueryExpression.From, predicate,
                    _sqlExpressionFactory.MakeUnary(ExpressionType.Equal, typeof(bool), outerPresence))
                : new LeftJoinExpression(inner.QueryExpression.From, predicate));
            outer.ShaperExpression = Projection(Bind(Lambda(method.Arguments[4]), left.ShaperExpression, right.ShaperExpression));
            if (full) PushdownIntoSubquery(outer, false);
            return outer;
        }

        ShapedQueryExpression TranslateLeftJoin(MethodCallExpression groupJoin, MethodCallExpression flatten)
        {
            if (flatten.Arguments.Count != 3 || groupJoin.Arguments.Count != 5) throw Unsupported(flatten);
            var outer = Translate(groupJoin.Arguments[0]);
            var inner = Translate(groupJoin.Arguments[1]);
            PushdownIntoSubquery(outer);
            MakeOptional(inner);
            var source = inner.QueryExpression.From;
            var predicate = JoinKeys(Bind(Lambda(groupJoin.Arguments[2]), outer.ShaperExpression), Bind(Lambda(groupJoin.Arguments[3]), inner.ShaperExpression));
            var groupSelector = Lambda(groupJoin.Arguments[4]);
            var grouping = new GroupByShaperExpression(groupSelector.Parameters[1].Type, null, inner.ShaperExpression);
            var transparent = Bind(groupSelector, outer.ShaperExpression, grouping);
            var collection = Bind(Lambda(flatten.Arguments[1]), transparent);
            if (!(collection is MethodCallExpression defaultIfEmpty) || defaultIfEmpty.Method.Name != nameof(Enumerable.DefaultIfEmpty)
                || defaultIfEmpty.Arguments.Count != 1 || !ReferenceEquals(defaultIfEmpty.Arguments[0], grouping)) throw Unsupported(flatten);
            var nullableInner = inner.ShaperExpression;
            outer.QueryExpression.Joins.Add(new LeftJoinExpression(source, predicate));
            outer.ShaperExpression = Projection(Bind(Lambda(flatten.Arguments[2]), transparent, nullableInner));
            return outer;
        }

        ShapedQueryExpression TranslateSetOperation(MethodCallExpression method)
        {
            if (method.Arguments.Count != 2) throw Unsupported(method);
            var left = Translate(method.Arguments[0]);
            var right = Translate(method.Arguments[1]);
            var aligned = AlignSetProjection(Projection(left.ShaperExpression), Projection(right.ShaperExpression));
            left.ShaperExpression = aligned.Key;
            right.ShaperExpression = aligned.Value;
            var alias = Alias();
            var leftLift = new ProjectionLifter(left.QueryExpression, alias);
            var projection = leftLift.Visit(left.ShaperExpression);
            var rightLift = new ProjectionLifter(right.QueryExpression, alias);
            rightLift.Visit(right.ShaperExpression);
            if (left.QueryExpression.Projection.Count != right.QueryExpression.Projection.Count) throw new NotSupportedException("Set operands must have compatible projections.");
            for (int i = 0; i < left.QueryExpression.Projection.Count; i++)
                if (left.QueryExpression.Projection[i].Expression.Type != right.QueryExpression.Projection[i].Expression.Type) throw new NotSupportedException("Set operands must have matching column types.");
            if (left.QueryExpression.Limit == null && left.QueryExpression.Offset == null) left.QueryExpression.Orderings.Clear();
            if (right.QueryExpression.Limit == null && right.QueryExpression.Offset == null) right.QueryExpression.Orderings.Clear();
            return new ShapedQueryExpression
            {
                QueryExpression = new SelectExpression { From = method.Method.Name == nameof(Queryable.Intersect)
                    ? (SetOperationBase)new IntersectExpression(alias, left.QueryExpression, right.QueryExpression)
                    : method.Method.Name == nameof(Queryable.Except) ? (SetOperationBase)new ExceptExpression(alias, left.QueryExpression, right.QueryExpression)
                    : new UnionExpression(alias, left.QueryExpression, right.QueryExpression, method.Method.Name != nameof(Queryable.Concat)) },
                ShaperExpression = projection,
            };
        }

        SqlExpression JoinKeys(Expression left, Expression right)
        {
            SqlExpression presence = null;
            while (left is OptionalShaperExpression leftOptional && !(leftOptional.Value is SqlExpression))
            { presence = And(presence, IsPresent(leftOptional.Presence)); left = leftOptional.Value; }
            while (right is OptionalShaperExpression rightOptional && !(rightOptional.Value is SqlExpression))
            { presence = And(presence, IsPresent(rightOptional.Presence)); right = rightOptional.Value; }
            if (left is NewExpression l && right is NewExpression r && l.Arguments.Count == r.Arguments.Count)
            {
                SqlExpression result = null;
                for (int i = 0; i < l.Arguments.Count; i++) result = And(result, _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), Scalar(l.Arguments[i]), Scalar(r.Arguments[i])));
                return And(presence, result);
            }
            // LINQ Join excludes null scalar keys, whereas structural keys compare their components.
            var a = Scalar(left); var b = Scalar(right);
            return And(presence, And(And(_sqlExpressionFactory.MakeUnary(ExpressionType.NotEqual, typeof(bool), a), _sqlExpressionFactory.MakeUnary(ExpressionType.NotEqual, typeof(bool), b)), _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), a, b, useRelationalNulls: true)));
        }

        static KeyValuePair<Expression, Expression> AlignSetProjection(Expression left, Expression right)
        {
            if (left.Type != right.Type) throw new NotSupportedException("Set operands must have matching projection types.");
            var leftEntity = StripConvert(left) as StructuralTypeProjectionExpression;
            var rightEntity = StripConvert(right) as StructuralTypeProjectionExpression;
            if (leftEntity?.Hierarchy != null && rightEntity?.Hierarchy != null
                && leftEntity.Hierarchy.Root.MappedType == rightEntity.Hierarchy.Root.MappedType)
            {
                var types = leftEntity.ConcreteTypes.Concat(rightEntity.ConcreteTypes).GroupBy(e => e.Mapping.MappedType).Select(g => g.First()).ToArray();
                if (!types.All(e => left.Type.IsAssignableFrom(e.Mapping.MappedType))) throw new NotSupportedException("Set operands have incompatible hierarchy result types.");
                // Only mapped entity result types participate in polymorphic set materialization.
                if (left.Type != typeof(object) && !left.Type.IsInterface)
                {
                    var mapping = SqlMapper.GetMapping(left.Type);
                    left = new StructuralTypeProjectionExpression(mapping, leftEntity.Members, leftEntity.Presence, leftEntity.Hierarchy, leftEntity.Discriminator, types);
                    right = new StructuralTypeProjectionExpression(mapping, rightEntity.Members, rightEntity.Presence, rightEntity.Hierarchy, rightEntity.Discriminator, types);
                }
            }
            if (left is OptionalShaperExpression || right is OptionalShaperExpression)
            {
                var lo = left as OptionalShaperExpression;
                var ro = right as OptionalShaperExpression;
                var values = AlignSetProjection(lo?.Value ?? left, ro?.Value ?? right);
                return new KeyValuePair<Expression, Expression>(
                    new OptionalShaperExpression(values.Key, lo?.Presence ?? new SqlConstantExpression(1, typeof(int?))),
                    new OptionalShaperExpression(values.Value, ro?.Presence ?? new SqlConstantExpression(1, typeof(int?))));
            }
            if (left is SqlExpression && right is SqlExpression) return new KeyValuePair<Expression, Expression>(left, right);
            if (left is UnaryExpression lu && right is UnaryExpression ru && lu.NodeType == ru.NodeType && lu.Method == ru.Method)
            {
                var pair = AlignSetProjection(lu.Operand, ru.Operand);
                return new KeyValuePair<Expression, Expression>(lu.Update(pair.Key), ru.Update(pair.Value));
            }
            if (left is StructuralTypeProjectionExpression le && right is StructuralTypeProjectionExpression re)
            {
                if (!le.Members.Keys.OrderBy(x => x).SequenceEqual(re.Members.Keys.OrderBy(x => x)))
                    throw new NotSupportedException("Set operands must have matching entity members.");
                return new KeyValuePair<Expression, Expression>(
                    le.Update(le.Members, le.Presence ?? (re.Presence == null ? null : new SqlConstantExpression(1, typeof(int?))), le.Discriminator),
                    re.Update(le.Members.Keys.ToDictionary(k => k, k => re.Members[k]), re.Presence ?? (le.Presence == null ? null : new SqlConstantExpression(1, typeof(int?))), re.Discriminator));
            }
            if (left is NewExpression ln && right is NewExpression rn && ln.Constructor == rn.Constructor)
            {
                var arguments = ln.Arguments.Zip(rn.Arguments, AlignSetProjection).ToArray();
                return new KeyValuePair<Expression, Expression>(ln.Update(arguments.Select(a => a.Key)), rn.Update(arguments.Select(a => a.Value)));
            }
            if (left is MemberInitExpression li && right is MemberInitExpression ri)
            {
                if (li.Bindings.Count != ri.Bindings.Count) throw new NotSupportedException("Set operands must initialize the same members.");
                var construction = AlignSetProjection(li.NewExpression, ri.NewExpression);
                var leftBindings = new List<MemberBinding>();
                var rightBindings = new List<MemberBinding>();
                foreach (var binding in li.Bindings.OfType<MemberAssignment>())
                {
                    var other = ri.Bindings.OfType<MemberAssignment>().SingleOrDefault(b => b.Member == binding.Member);
                    if (other == null) throw new NotSupportedException("Set operands must initialize the same members.");
                    var pair = AlignSetProjection(binding.Expression, other.Expression);
                    leftBindings.Add(Expression.Bind(binding.Member, pair.Key));
                    rightBindings.Add(Expression.Bind(other.Member, pair.Value));
                }
                return new KeyValuePair<Expression, Expression>(Expression.MemberInit((NewExpression)construction.Key, leftBindings), Expression.MemberInit((NewExpression)construction.Value, rightBindings));
            }
            throw new NotSupportedException("Set operands must have matching projection shapes.");
        }

        internal void PushdownIntoSubquery(ShapedQueryExpression source, bool liftOrderings = true)
        {
            var inner = source.QueryExpression;
            var alias = Alias();
            var lift = new ProjectionLifter(inner, alias);
            source.ShaperExpression = lift.Visit(source.ShaperExpression);
            inner.Alias = alias;
            var outer = new SelectExpression { From = inner };
            if (liftOrderings && !inner.IsDistinct)
                foreach (var ordering in inner.Orderings)
                    outer.Orderings.Add(new OrderingExpression((SqlExpression)lift.Visit(ordering.Expression), ordering.IsAscending));
            if (inner.Limit == null && inner.Offset == null) inner.Orderings.Clear();
            source.QueryExpression = outer;
        }

        internal Expression Projection(Expression expression)
        {
            if (expression is OptionalShaperExpression optional)
                return optional.Value is SqlExpression ? (Expression)Scalar(optional) : new OptionalShaperExpression(Projection(optional.Value), optional.Presence);
            if (expression is StructuralTypeProjectionExpression || expression is GroupByShaperExpression) return expression;
            if (expression is UnaryExpression conversion && (conversion.NodeType == ExpressionType.Convert || conversion.NodeType == ExpressionType.ConvertChecked)
                && (conversion.Type == typeof(object) || conversion.Type.IsAssignableFrom(conversion.Operand.Type)))
                return conversion.Update(Projection(conversion.Operand));
            if (expression is NewExpression created)
                return created.Update(created.Arguments.Select(Projection));
            if (expression is MemberInitExpression init)
                return Expression.MemberInit((NewExpression)Projection(init.NewExpression), init.Bindings.Select(b => b is MemberAssignment assignment
                    ? Expression.Bind(b.Member, Projection(assignment.Expression)) : throw Unsupported(expression)));
            return Scalar(expression);
        }

        internal SqlExpression Scalar(Expression expression) => _sqlTranslator.Translate(expression);
        internal static Expression StripQuote(Expression expression)
        { while (expression.NodeType == ExpressionType.Quote) expression = ((UnaryExpression)expression).Operand; return expression; }
        internal static Expression StripConvert(Expression expression)
        { while (expression is UnaryExpression u && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked)) expression = u.Operand; return expression; }
        internal static LambdaExpression Lambda(Expression expression)
        {
            if (!(StripQuote(expression) is LambdaExpression lambda) || lambda.Parameters.Count > 2) throw Unsupported(expression);
            return lambda;
        }
        internal static Expression Bind(LambdaExpression lambda, params Expression[] arguments)
        {
            if (lambda.Parameters.Count != arguments.Length) throw Unsupported(lambda);
            return new ProjectionBinder(lambda.Parameters.Zip(arguments, (parameter, argument) => new { parameter, argument })
                .ToDictionary(x => x.parameter, x => x.argument)).Visit(lambda.Body);
        }
        internal static NotSupportedException Unsupported(Expression expression)
            => new NotSupportedException("Cannot translate expression to SQL: " + expression + " (" + expression.GetType().Name + "). Use AsEnumerable() for explicit client evaluation.");

        internal static IEnumerable<SqlExpression> Leaves(Expression expression)
        {
            var query = new SelectExpression();
            new ProjectionLifter(query, "unused").Visit(expression);
            return query.Projection.Select(p => p.Expression);
        }

        private sealed class ProjectionBinder : ExpressionVisitor
        {
            readonly IReadOnlyDictionary<ParameterExpression, Expression> _bindings;
            internal ProjectionBinder(IReadOnlyDictionary<ParameterExpression, Expression> bindings)
            {
                _bindings = bindings;
            }
            protected override Expression VisitParameter(ParameterExpression node) => _bindings.TryGetValue(node, out var value) ? value : node;
            protected override Expression VisitExtension(Expression node) => node;
            protected override Expression VisitMember(MemberExpression node)
            {
                var source = node.Expression == null ? null : StripConvert(Visit(node.Expression));
                if (source is OptionalShaperExpression optional)
                    return new OptionalShaperExpression(Visit(node.Update(optional.Value)), optional.Presence);
                if (source is StructuralTypeProjectionExpression entity && entity.Members.TryGetValue(node.Member.Name, out var member)) return member;
                if (source is GroupByShaperExpression group && node.Member.Name == "Key") return group.Key;
                if (source is NewExpression created && created.Members != null)
                {
                    var index = created.Members.IndexOf(node.Member);
                    if (index >= 0) return created.Arguments[index];
                }
                if (source is MemberInitExpression init)
                {
                    var assignment = init.Bindings.OfType<MemberAssignment>().FirstOrDefault(b => b.Member == node.Member);
                    if (assignment != null) return assignment.Expression;
                }
                return node.Update(source);
            }
        }

        private sealed class ProjectionLifter : ExpressionVisitor
        {
            readonly SelectExpression _query;
            readonly string _alias;
            internal ProjectionLifter(SelectExpression query, string alias)
            {
                _query = query;
                _alias = alias;
            }
            protected override Expression VisitExtension(Expression node)
            {
                if (node is OptionalShaperExpression optional)
                    return new OptionalShaperExpression(Visit(optional.Value), (SqlExpression)Visit(optional.Presence));
                if (node is SqlExpression sql)
                {
                    var ordinal = _query.AddToProjection(sql);
                    return new ColumnExpression(_alias, "c" + ordinal, sql.Type, sql.IsNullable);
                }
                if (node is StructuralTypeProjectionExpression entity)
                    return entity.Update(entity.Members.ToDictionary(p => p.Key, p => Visit(p.Value)), (SqlExpression)Visit(entity.Presence), (SqlExpression)Visit(entity.Discriminator));
                throw new NotSupportedException("Project GroupBy keys and aggregates before composing this operation.");
            }
        }

        private sealed class NullableProjectionVisitor : ExpressionVisitor
        {
            protected override Expression VisitExtension(Expression node)
            {
                if (node is ColumnExpression column) return new ColumnExpression(column.TableAlias, column.Name, column.Type, true);
                if (node is StructuralTypeProjectionExpression entity)
                    return entity.Update(entity.Members.ToDictionary(p => p.Key, p => Visit(p.Value)), (SqlExpression)Visit(entity.Presence), (SqlExpression)Visit(entity.Discriminator));
                return base.VisitExtension(node);
            }
        }

        private sealed class CorrelationDetector : ExpressionVisitor
        {
            internal bool Found;
            protected override Expression VisitExtension(Expression node)
            {
                if (node is ColumnExpression) Found = true;
                return node;
            }
        }
    }
}
