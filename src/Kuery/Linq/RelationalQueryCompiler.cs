using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kuery.Linq
{
    internal sealed class RelationalQueryCompiler
    {
        int _alias;
        internal string Alias() => "t" + _alias++;
        internal static SqlConstantExpression Integer(int value) => new SqlConstantExpression(value, typeof(int));
        internal static SqlOperationExpression Op(string operation, Type type, params SqlExpression[] args)
            => new SqlOperationExpression(operation, type, args.Any(a => a.IsNullable), args);
        internal static SqlExpression And(SqlExpression left, SqlExpression right)
            => left == null ? right : Op("AndAlso", typeof(bool), left, right);

        internal RelationalQueryPlan Compile(Expression expression)
        {
            var plan = Query(expression);
            plan.ElementType = plan.Projection.Type;
            plan.Materialize = ProjectionReader.Compile(plan.Query, plan.Projection);
            return plan;
        }

        internal RelationalQueryPlan CompileMutation(Expression expression, string operation, IReadOnlyList<SetPropertyCall> setters)
        {
            // Validate the syntax as well as the resulting SQL shape: ignored operators must not broaden a mutation.
            var root = expression;
            while (root is MethodCallExpression call && call.Method.DeclaringType == typeof(Queryable) && call.Method.Name == nameof(Queryable.Where))
                root = call.Arguments[0];
            if (!(root is QueryRootExpression)) throw new NotSupportedException(operation + " supports only a single table with Where conditions.");
            var plan = Query(expression);
            if (plan.Query.Predicate == null) throw new InvalidOperationException("No condition specified");
            plan.Mutation = operation;
            plan.MutationTable = ((EntityProjectionExpression)plan.Projection).Mapping;
            if (operation == "update")
            {
                if (setters == null || setters.Count == 0) throw new InvalidOperationException("No setters specified");
                var names = new HashSet<string>();
                foreach (var setter in setters)
                {
                    var property = StripConvert(setter.PropertyExpression.Body) as MemberExpression;
                    if (property == null || property.Expression != setter.PropertyExpression.Parameters[0])
                        throw new NotSupportedException("SetProperty requires a direct mapped property access.");
                    var column = plan.MutationTable.FindColumnWithPropertyName(property.Member.Name);
                    if (column == null || !names.Add(column.Name)) throw new NotSupportedException("Unknown or duplicate update property: " + property.Member.Name);
                    plan.Setters.Add(new KeyValuePair<string, SqlExpression>(column.Name, Scalar(Bind(setter.ValueExpression, plan.Projection))));
                }
            }
            return plan;
        }

        internal RelationalQueryPlan Query(Expression expression)
        {
            if (expression is QueryRootExpression root)
            {
                var alias = Alias();
                var members = root.Mapping.Columns.ToDictionary(c => c.PropertyName, c =>
                {
                    var type = root.Mapping.MappedType.GetProperty(c.PropertyName).PropertyType;
                    return (Expression)new SqlColumnExpression(alias, c.Name, type, c.IsNullable && (!type.IsValueType || Nullable.GetUnderlyingType(type) != null));
                });
                return new RelationalQueryPlan
                {
                    Query = new SqlSelectExpression { From = new SqlTableSource { TableName = root.Mapping.TableName, Alias = alias } },
                    Projection = new EntityProjectionExpression(root.Mapping, members),
                };
            }
            if (!(expression is MethodCallExpression method) || method.Method.DeclaringType != typeof(Queryable))
                throw Unsupported(expression);
            var name = method.Method.Name;
            if (name == nameof(Queryable.SelectMany) && method.Arguments[0] is MethodCallExpression groupJoin
                && groupJoin.Method.DeclaringType == typeof(Queryable) && groupJoin.Method.Name == nameof(Queryable.GroupJoin))
                return LeftJoin(groupJoin, method);
            if (name == nameof(Queryable.Join)) return Join(method);
            if (name == nameof(Queryable.Union) || name == nameof(Queryable.Concat) || name == nameof(Queryable.Intersect) || name == nameof(Queryable.Except))
                return Set(method);
            var plan = Query(method.Arguments[0]);
            var query = plan.Query;
            switch (name)
            {
                case nameof(Queryable.Where):
                    Where(plan, Lambda(method.Arguments[1]));
                    break;
                case nameof(Queryable.Select):
                    if (query.Distinct) Wrap(plan);
                    plan.Projection = Projection(Bind(Lambda(method.Arguments[1]), plan.Projection));
                    break;
                case nameof(Queryable.OrderBy):
                case nameof(Queryable.OrderByDescending):
                case nameof(Queryable.ThenBy):
                case nameof(Queryable.ThenByDescending):
                    if (method.Arguments.Count != 2) throw Unsupported(method);
                    if (query.Limit != null || query.Offset != null || query.Distinct) Wrap(plan);
                    if (name == nameof(Queryable.OrderBy) || name == nameof(Queryable.OrderByDescending)) plan.Query.Orderings.Clear();
                    plan.Query.Orderings.Add(new SqlOrderingExpression(Scalar(Bind(Lambda(method.Arguments[1]), plan.Projection)),
                        name == nameof(Queryable.OrderBy) || name == nameof(Queryable.ThenBy)));
                    break;
                case nameof(Queryable.Skip):
                    if (method.Arguments[1].Type != typeof(int)) throw Unsupported(method);
                    // A subquery avoids arithmetic overflow and preserves Take().Skip() exactly.
                    if (query.Limit != null || query.Offset != null) Wrap(plan);
                    plan.Query.Offset = Op("NonNegative", typeof(int), Scalar(method.Arguments[1]));
                    break;
                case nameof(Queryable.Take):
                    if (method.Arguments[1].Type != typeof(int)) throw Unsupported(method);
                    var take = Op("NonNegative", typeof(int), Scalar(method.Arguments[1]));
                    query.Limit = query.Limit == null ? take : Op("Least", typeof(int), query.Limit, take);
                    break;
                case nameof(Queryable.Distinct):
                    if (method.Arguments.Count != 1) throw Unsupported(method);
                    if (query.Limit != null || query.Offset != null) Wrap(plan);
                    plan.Query.Distinct = true;
                    plan.Query.Orderings.Clear();
                    break;
                case nameof(Queryable.GroupBy):
                    if (method.Arguments.Count != 2) throw Unsupported(method);
                    if (query.Limit != null || query.Offset != null || query.Distinct || query.GroupBy.Count > 0) Wrap(plan);
                    var key = Projection(Bind(Lambda(method.Arguments[1]), plan.Projection));
                    plan.Query.GroupBy.AddRange(Leaves(key));
                    plan.Query.Orderings.Clear();
                    plan.Projection = new GroupProjectionExpression(method.Type.GetGenericArguments()[0], key, plan.Projection);
                    break;
                case nameof(Queryable.SelectMany):
                    var collection = Bind(Lambda(method.Arguments[1]), plan.Projection);
                    var correlation = new CorrelationDetector();
                    correlation.Visit(collection);
                    if (correlation.Found) throw new NotSupportedException("Correlated SelectMany requires lateral joins. Use a correlated Any/Contains subquery or an explicit Join.");
                    var inner = Query(collection);
                    Wrap(plan);
                    Wrap(inner, false);
                    plan.Query.Joins.Add(new SqlJoinExpression { Kind = "cross", Source = inner.Query.From });
                    plan.Projection = method.Arguments.Count == 3
                        ? Projection(Bind(Lambda(method.Arguments[2]), plan.Projection, inner.Projection)) : inner.Projection;
                    break;
                case nameof(Queryable.Count):
                case nameof(Queryable.LongCount):
                case nameof(Queryable.Sum):
                case nameof(Queryable.Min):
                case nameof(Queryable.Max):
                case nameof(Queryable.Average):
                    if ((name == nameof(Queryable.Count) || name == nameof(Queryable.LongCount)) && method.Arguments.Count == 2)
                        Where(plan, Lambda(method.Arguments[1]));
                    if ((name == nameof(Queryable.Count) || name == nameof(Queryable.LongCount)) && plan.Projection is GroupProjectionExpression countGroup)
                        plan.Projection = countGroup.Key;
                    if (plan.Query.Distinct || plan.Query.Limit != null || plan.Query.Offset != null || plan.Query.GroupBy.Count > 0) Wrap(plan);
                    SqlExpression argument = null;
                    if (name != nameof(Queryable.Count) && name != nameof(Queryable.LongCount))
                        argument = Scalar(method.Arguments.Count == 2 ? Bind(Lambda(method.Arguments[1]), plan.Projection) : plan.Projection);
                    plan.Query.Orderings.Clear();
                    var aggregate = new SqlOperationExpression(name, method.Type, true, argument == null ? Array.Empty<SqlExpression>() : new[] { argument });
                    plan.Projection = name == nameof(Queryable.Sum)
                        ? Op("Coalesce", method.Type, aggregate, new SqlConstantExpression(Activator.CreateInstance(Nullable.GetUnderlyingType(method.Type) ?? method.Type), method.Type)) : aggregate;
                    plan.ResultKind = QueryResultKind.Scalar;
                    break;
                case nameof(Queryable.Any):
                case nameof(Queryable.All):
                    if (method.Arguments.Count == 2)
                    {
                        if (name == nameof(Queryable.Any)) Where(plan, Lambda(method.Arguments[1]));
                        else
                        {
                            if (query.Limit != null || query.Offset != null || query.Distinct) Wrap(plan);
                            var negated = Op("Not", typeof(bool), Scalar(Bind(Lambda(method.Arguments[1]), plan.Projection)));
                            if (plan.Projection is GroupProjectionExpression) plan.Query.Having = And(plan.Query.Having, negated);
                            else if (plan.Query.GroupBy.Count > 0)
                            {
                                Wrap(plan);
                                plan.Query.Predicate = Op("Not", typeof(bool), Scalar(Bind(Lambda(method.Arguments[1]), plan.Projection)));
                            }
                            else plan.Query.Predicate = And(plan.Query.Predicate, negated);
                        }
                    }
                    if (plan.Projection is GroupProjectionExpression existsGroup) plan.Projection = existsGroup.Key;
                    if (plan.Query.GroupBy.Count > 0 || plan.Query.Distinct) Wrap(plan);
                    plan.Query.Projections.Add(Integer(1));
                    if (plan.Query.Limit == null && plan.Query.Offset == null) plan.Query.Orderings.Clear();
                    plan.Projection = new SqlSubqueryExpression(name == nameof(Queryable.Any) ? "exists" : "not exists", plan.Query, typeof(bool));
                    plan.Query = new SqlSelectExpression();
                    plan.ResultKind = QueryResultKind.Scalar;
                    break;
                case nameof(Queryable.First):
                case nameof(Queryable.FirstOrDefault):
                case nameof(Queryable.Single):
                case nameof(Queryable.SingleOrDefault):
                case nameof(Queryable.Last):
                case nameof(Queryable.LastOrDefault):
                case nameof(Queryable.ElementAt):
                case nameof(Queryable.ElementAtOrDefault):
                    if (name.StartsWith("ElementAt", StringComparison.Ordinal))
                    {
                        if (method.Arguments[1].Type != typeof(int)) throw Unsupported(method);
                        if (query.Limit != null || query.Offset != null) Wrap(plan);
                        // Negative indices produce no rows; the result cardinality determines throw/default.
                        var index = Scalar(method.Arguments[1]);
                        plan.Query.Predicate = And(plan.Query.Predicate, Op("GreaterThanOrEqual", typeof(bool), index, Integer(0)));
                        plan.Query.Offset = Op("NonNegative", typeof(int), index);
                    }
                    else if (method.Arguments.Count == 2)
                    {
                        if (StripQuote(method.Arguments[1]) is LambdaExpression predicate) Where(plan, predicate);
                        else throw Unsupported(method);
                    }
                    if (name.StartsWith("Last", StringComparison.Ordinal))
                    {
                        if (plan.Query.Orderings.Count == 0) throw new NotSupportedException("Last requires an explicit OrderBy.");
                        if (plan.Query.Limit != null || plan.Query.Offset != null) Wrap(plan);
                        foreach (var ordering in plan.Query.Orderings) ordering.Ascending = !ordering.Ascending;
                    }
                    bool single = name.StartsWith("Single", StringComparison.Ordinal);
                    var limit = Integer(single ? 2 : 1);
                    plan.Query.Limit = plan.Query.Limit == null ? (SqlExpression)limit : Op("Least", typeof(int), plan.Query.Limit, limit);
                    plan.ResultKind = single
                        ? (name.EndsWith("OrDefault", StringComparison.Ordinal) ? QueryResultKind.SingleOrDefault : QueryResultKind.Single)
                        : (name.EndsWith("OrDefault", StringComparison.Ordinal) ? QueryResultKind.FirstOrDefault : QueryResultKind.First);
                    if (name == nameof(Queryable.ElementAt)) plan.ResultKind = QueryResultKind.ElementAt;
                    if (name == nameof(Queryable.ElementAtOrDefault)) plan.ResultKind = QueryResultKind.ElementAtOrDefault;
                    break;
                default: throw Unsupported(method);
            }
            return plan;
        }

        void Where(RelationalQueryPlan plan, LambdaExpression predicate)
        {
            if (plan.Query.Limit != null || plan.Query.Offset != null || plan.Query.Distinct) Wrap(plan);
            var sql = Scalar(Bind(predicate, plan.Projection));
            if (plan.Projection is GroupProjectionExpression) plan.Query.Having = And(plan.Query.Having, sql);
            else if (plan.Query.GroupBy.Count > 0) { Wrap(plan); plan.Query.Predicate = Scalar(Bind(predicate, plan.Projection)); }
            else plan.Query.Predicate = And(plan.Query.Predicate, sql);
        }

        RelationalQueryPlan Join(MethodCallExpression method)
        {
            if (method.Arguments.Count != 5) throw Unsupported(method);
            var outer = Query(method.Arguments[0]);
            var inner = Query(method.Arguments[1]);
            Wrap(outer);
            Wrap(inner, false);
            var predicate = JoinKeys(Bind(Lambda(method.Arguments[2]), outer.Projection), Bind(Lambda(method.Arguments[3]), inner.Projection));
            outer.Query.Joins.Add(new SqlJoinExpression { Kind = "inner", Source = inner.Query.From, Predicate = predicate });
            outer.Projection = Projection(Bind(Lambda(method.Arguments[4]), outer.Projection, inner.Projection));
            return outer;
        }

        RelationalQueryPlan LeftJoin(MethodCallExpression groupJoin, MethodCallExpression flatten)
        {
            if (flatten.Arguments.Count != 3 || groupJoin.Arguments.Count != 5) throw Unsupported(flatten);
            var outer = Query(groupJoin.Arguments[0]);
            var inner = Query(groupJoin.Arguments[1]);
            Wrap(outer);
            var innerShape = inner.Projection;
            var presence = new SqlConstantExpression(1, typeof(int?));
            // Add a non-null sentinel inside the derived table, so a matched all-NULL row is distinguishable.
            var innerAlias = Alias();
            var lift = new ProjectionLifter(inner.Query, innerAlias);
            inner.Projection = lift.Visit(innerShape);
            var marker = (SqlExpression)lift.Visit(presence);
            if (inner.Query.Limit == null && inner.Query.Offset == null) inner.Query.Orderings.Clear();
            var source = new SqlTableSource { Query = inner.Query, Alias = innerAlias };
            var predicate = JoinKeys(Bind(Lambda(groupJoin.Arguments[2]), outer.Projection), Bind(Lambda(groupJoin.Arguments[3]), inner.Projection));
            var groupSelector = Lambda(groupJoin.Arguments[4]);
            var grouping = new GroupProjectionExpression(groupSelector.Parameters[1].Type, null, inner.Projection);
            var transparent = Bind(groupSelector, outer.Projection, grouping);
            var collection = Bind(Lambda(flatten.Arguments[1]), transparent);
            if (!(collection is MethodCallExpression defaultIfEmpty) || defaultIfEmpty.Method.Name != nameof(Enumerable.DefaultIfEmpty)
                || defaultIfEmpty.Arguments.Count != 1 || !ReferenceEquals(defaultIfEmpty.Arguments[0], grouping)) throw Unsupported(flatten);
            var nullableInner = new NullableProjectionVisitor(marker).Visit(inner.Projection);
            outer.Query.Joins.Add(new SqlJoinExpression { Kind = "left", Source = source, Predicate = predicate });
            outer.Projection = Projection(Bind(Lambda(flatten.Arguments[2]), transparent, nullableInner));
            return outer;
        }

        RelationalQueryPlan Set(MethodCallExpression method)
        {
            if (method.Arguments.Count != 2) throw Unsupported(method);
            var left = Query(method.Arguments[0]);
            var right = Query(method.Arguments[1]);
            var aligned = AlignSetProjection(left.Projection, right.Projection);
            left.Projection = aligned.Key;
            right.Projection = aligned.Value;
            var alias = Alias();
            var leftLift = new ProjectionLifter(left.Query, alias);
            var projection = leftLift.Visit(left.Projection);
            var rightLift = new ProjectionLifter(right.Query, alias);
            rightLift.Visit(right.Projection);
            if (left.Query.Projections.Count != right.Query.Projections.Count) throw new NotSupportedException("Set operands must have compatible projections.");
            for (int i = 0; i < left.Query.Projections.Count; i++)
                if (left.Query.Projections[i].Type != right.Query.Projections[i].Type) throw new NotSupportedException("Set operands must have matching column types.");
            if (left.Query.Limit == null && left.Query.Offset == null) left.Query.Orderings.Clear();
            if (right.Query.Limit == null && right.Query.Offset == null) right.Query.Orderings.Clear();
            return new RelationalQueryPlan
            {
                Query = new SqlSelectExpression { From = new SqlTableSource { Alias = alias, Left = left.Query, Right = right.Query,
                    SetOperator = method.Method.Name == nameof(Queryable.Concat) ? "union all" : method.Method.Name.ToLowerInvariant() } },
                Projection = projection,
            };
        }

        SqlExpression JoinKeys(Expression left, Expression right)
        {
            if (left is NewExpression l && right is NewExpression r && l.Arguments.Count == r.Arguments.Count)
            {
                SqlExpression result = null;
                for (int i = 0; i < l.Arguments.Count; i++) result = And(result, Op("Equal", typeof(bool), Scalar(l.Arguments[i]), Scalar(r.Arguments[i])));
                return result;
            }
            // LINQ Join excludes null scalar keys, whereas structural keys compare their components.
            var a = Scalar(left); var b = Scalar(right);
            return And(And(Op("IsNotNull", typeof(bool), a), Op("IsNotNull", typeof(bool), b)), Op("SqlEqual", typeof(bool), a, b));
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

        internal void Wrap(RelationalQueryPlan plan, bool liftOrderings = true)
        {
            var inner = plan.Query;
            var alias = Alias();
            var lift = new ProjectionLifter(inner, alias);
            plan.Projection = lift.Visit(plan.Projection);
            var outer = new SqlSelectExpression { From = new SqlTableSource { Query = inner, Alias = alias } };
            if (liftOrderings && !inner.Distinct)
                foreach (var ordering in inner.Orderings)
                    outer.Orderings.Add(new SqlOrderingExpression((SqlExpression)lift.Visit(ordering.Expression), ordering.Ascending));
            if (inner.Limit == null && inner.Offset == null) inner.Orderings.Clear();
            plan.Query = outer;
        }

        internal Expression Projection(Expression expression)
        {
            if (expression is EntityProjectionExpression || expression is GroupProjectionExpression) return expression;
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

        internal SqlExpression Scalar(Expression expression) => new SqlScalarTranslator(this).Translate(expression);
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
            var query = new SqlSelectExpression();
            new ProjectionLifter(query, "unused").Visit(expression);
            return query.Projections;
        }

        private sealed class ProjectionBinder : ExpressionVisitor
        {
            readonly IReadOnlyDictionary<ParameterExpression, Expression> _bindings;
            internal ProjectionBinder(IReadOnlyDictionary<ParameterExpression, Expression> bindings) { _bindings = bindings; }
            protected override Expression VisitParameter(ParameterExpression node) => _bindings.TryGetValue(node, out var value) ? value : node;
            protected override Expression VisitExtension(Expression node) => node;
            protected override Expression VisitMember(MemberExpression node)
            {
                var source = node.Expression == null ? null : Visit(node.Expression);
                if (source is EntityProjectionExpression entity && entity.Members.TryGetValue(node.Member.Name, out var member)) return member;
                if (source is GroupProjectionExpression group && node.Member.Name == "Key") return group.Key;
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
            readonly SqlSelectExpression _query;
            readonly string _alias;
            internal ProjectionLifter(SqlSelectExpression query, string alias) { _query = query; _alias = alias; }
            protected override Expression VisitExtension(Expression node)
            {
                if (node is SqlExpression sql)
                {
                    var ordinal = _query.Projections.Count;
                    _query.Projections.Add(sql);
                    return new SqlColumnExpression(_alias, "c" + ordinal, sql.Type, sql.IsNullable);
                }
                if (node is EntityProjectionExpression entity)
                    return new EntityProjectionExpression(entity.Mapping, entity.Members.ToDictionary(p => p.Key, p => Visit(p.Value)), (SqlExpression)Visit(entity.Presence));
                throw new NotSupportedException("Project GroupBy keys and aggregates before composing this operation.");
            }
        }

        private sealed class NullableProjectionVisitor : ExpressionVisitor
        {
            readonly SqlExpression _presence;
            internal NullableProjectionVisitor(SqlExpression presence) { _presence = presence; }
            protected override Expression VisitExtension(Expression node)
            {
                if (node is EntityProjectionExpression entity)
                    return new EntityProjectionExpression(entity.Mapping, entity.Members.ToDictionary(p => p.Key, p => Visit(p.Value)), _presence);
                if (node is SqlColumnExpression column) return new SqlColumnExpression(column.Alias, column.Name, column.Type, true);
                return node;
            }
        }

        private sealed class CorrelationDetector : ExpressionVisitor
        {
            internal bool Found;
            protected override Expression VisitExtension(Expression node)
            {
                if (node is SqlColumnExpression) Found = true;
                return node;
            }
        }
    }
}
