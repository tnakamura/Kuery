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
            if (!(root is QueryRootExpression)) throw new NotSupportedException(operation + " supports only a single table with Where conditions.");
            var source = Translate(expression);
            if (source.QueryExpression.Predicate == null) throw new InvalidOperationException("No condition specified");
            var nonQuery = new NonQueryExpression { Operation = operation, QueryExpression = source.QueryExpression, Table = ((EntityProjectionExpression)source.ShaperExpression).Mapping };
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
            var members = root.Mapping.Columns.ToDictionary(c => c.PropertyName, c =>
            {
                var type = root.Mapping.MappedType.GetProperty(c.PropertyName).PropertyType;
                return (Expression)new ColumnExpression(alias, c.Name, type, c.IsNullable && (!type.IsValueType || Nullable.GetUnderlyingType(type) != null));
            });
            return new ShapedQueryExpression
            {
                QueryExpression = new SelectExpression { From = new TableExpression(root.Mapping.TableName, alias) },
                ShaperExpression = new EntityProjectionExpression(root.Mapping, members),
            };
        }

        protected override Expression VisitMethodCall(MethodCallExpression method)
        {
            if (method.Method.DeclaringType != typeof(Queryable)) throw Unsupported(method);
            var name = method.Method.Name;
            if (name == nameof(Queryable.SelectMany) && method.Arguments[0] is MethodCallExpression groupJoin
                && groupJoin.Method.DeclaringType == typeof(Queryable) && groupJoin.Method.Name == nameof(Queryable.GroupJoin))
                return TranslateLeftJoin(groupJoin, method);
            if (name == nameof(Queryable.Join)) return TranslateJoin(method);
            if (name == nameof(Queryable.Union) || name == nameof(Queryable.Concat) || name == nameof(Queryable.Intersect) || name == nameof(Queryable.Except))
                return TranslateSetOperation(method);
            var source = Translate(method.Arguments[0]);
            switch (name)
            {
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

        ShapedQueryExpression TranslateLeftJoin(MethodCallExpression groupJoin, MethodCallExpression flatten)
        {
            if (flatten.Arguments.Count != 3 || groupJoin.Arguments.Count != 5) throw Unsupported(flatten);
            var outer = Translate(groupJoin.Arguments[0]);
            var inner = Translate(groupJoin.Arguments[1]);
            PushdownIntoSubquery(outer);
            var innerShape = inner.ShaperExpression;
            var presence = new SqlConstantExpression(1, typeof(int?));
            // Add a non-null sentinel inside the derived table, so a matched all-NULL row is distinguishable.
            var innerAlias = Alias();
            var lift = new ProjectionLifter(inner.QueryExpression, innerAlias);
            inner.ShaperExpression = lift.Visit(innerShape);
            var marker = (SqlExpression)lift.Visit(presence);
            if (inner.QueryExpression.Limit == null && inner.QueryExpression.Offset == null) inner.QueryExpression.Orderings.Clear();
            inner.QueryExpression.Alias = innerAlias;
            var source = inner.QueryExpression;
            var predicate = JoinKeys(Bind(Lambda(groupJoin.Arguments[2]), outer.ShaperExpression), Bind(Lambda(groupJoin.Arguments[3]), inner.ShaperExpression));
            var groupSelector = Lambda(groupJoin.Arguments[4]);
            var grouping = new GroupByShaperExpression(groupSelector.Parameters[1].Type, null, inner.ShaperExpression);
            var transparent = Bind(groupSelector, outer.ShaperExpression, grouping);
            var collection = Bind(Lambda(flatten.Arguments[1]), transparent);
            if (!(collection is MethodCallExpression defaultIfEmpty) || defaultIfEmpty.Method.Name != nameof(Enumerable.DefaultIfEmpty)
                || defaultIfEmpty.Arguments.Count != 1 || !ReferenceEquals(defaultIfEmpty.Arguments[0], grouping)) throw Unsupported(flatten);
            var nullableInner = new NullableProjectionVisitor(marker).Visit(inner.ShaperExpression);
            outer.QueryExpression.Joins.Add(new LeftJoinExpression(source, predicate));
            outer.ShaperExpression = Projection(Bind(Lambda(flatten.Arguments[2]), transparent, nullableInner));
            return outer;
        }

        ShapedQueryExpression TranslateSetOperation(MethodCallExpression method)
        {
            if (method.Arguments.Count != 2) throw Unsupported(method);
            var left = Translate(method.Arguments[0]);
            var right = Translate(method.Arguments[1]);
            var aligned = AlignSetProjection(left.ShaperExpression, right.ShaperExpression);
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
            if (left is NewExpression l && right is NewExpression r && l.Arguments.Count == r.Arguments.Count)
            {
                SqlExpression result = null;
                for (int i = 0; i < l.Arguments.Count; i++) result = And(result, _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), Scalar(l.Arguments[i]), Scalar(r.Arguments[i])));
                return result;
            }
            // LINQ Join excludes null scalar keys, whereas structural keys compare their components.
            var a = Scalar(left); var b = Scalar(right);
            return And(And(_sqlExpressionFactory.MakeUnary(ExpressionType.NotEqual, typeof(bool), a), _sqlExpressionFactory.MakeUnary(ExpressionType.NotEqual, typeof(bool), b)), _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), a, b, useRelationalNulls: true));
        }

        static KeyValuePair<Expression, Expression> AlignSetProjection(Expression left, Expression right)
        {
            if (left.Type != right.Type) throw new NotSupportedException("Set operands must have matching projection types.");
            if (left is SqlExpression && right is SqlExpression) return new KeyValuePair<Expression, Expression>(left, right);
            if (left is UnaryExpression lu && right is UnaryExpression ru && lu.NodeType == ru.NodeType && lu.Method == ru.Method)
            {
                var pair = AlignSetProjection(lu.Operand, ru.Operand);
                return new KeyValuePair<Expression, Expression>(lu.Update(pair.Key), ru.Update(pair.Value));
            }
            if (left is EntityProjectionExpression le && right is EntityProjectionExpression re)
            {
                if (!le.Members.Keys.OrderBy(x => x).SequenceEqual(re.Members.Keys.OrderBy(x => x)))
                    throw new NotSupportedException("Set operands must have matching entity members.");
                return new KeyValuePair<Expression, Expression>(
                    new EntityProjectionExpression(le.Mapping, le.Members, le.Presence ?? (re.Presence == null ? null : new SqlConstantExpression(1, typeof(int?)))),
                    new EntityProjectionExpression(re.Mapping, le.Members.Keys.ToDictionary(k => k, k => re.Members[k]), re.Presence ?? (le.Presence == null ? null : new SqlConstantExpression(1, typeof(int?)))));
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
            if (expression is EntityProjectionExpression || expression is GroupByShaperExpression) return expression;
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
                var source = node.Expression == null ? null : Visit(node.Expression);
                if (source is EntityProjectionExpression entity && entity.Members.TryGetValue(node.Member.Name, out var member)) return member;
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
                if (node is SqlExpression sql)
                {
                    var ordinal = _query.AddToProjection(sql);
                    return new ColumnExpression(_alias, "c" + ordinal, sql.Type, sql.IsNullable);
                }
                if (node is EntityProjectionExpression entity)
                    return new EntityProjectionExpression(entity.Mapping, entity.Members.ToDictionary(p => p.Key, p => Visit(p.Value)), (SqlExpression)Visit(entity.Presence));
                throw new NotSupportedException("Project GroupBy keys and aggregates before composing this operation.");
            }
        }

        private sealed class NullableProjectionVisitor : ExpressionVisitor
        {
            readonly SqlExpression _presence;
            internal NullableProjectionVisitor(SqlExpression presence)
            {
                _presence = presence;
            }
            protected override Expression VisitExtension(Expression node)
            {
                if (node is EntityProjectionExpression entity)
                    return new EntityProjectionExpression(entity.Mapping, entity.Members.ToDictionary(p => p.Key, p => Visit(p.Value)), _presence);
                if (node is ColumnExpression column) return new ColumnExpression(column.TableAlias, column.Name, column.Type, true);
                return node;
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
