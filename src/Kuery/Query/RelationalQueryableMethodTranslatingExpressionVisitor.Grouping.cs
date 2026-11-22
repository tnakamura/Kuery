using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <remarks>Grouping translation can project ranked rows and coordinate collection materialization.</remarks>
    internal sealed partial class RelationalQueryableMethodTranslatingExpressionVisitor
    {
        void CompleteGrouping(ShapedQueryExpression source, GroupByShaperExpression grouping)
        {
            var keys = PrepareGroupingRows(source);
            var identifier = new WindowFunctionExpression(WindowFunction.DenseRank, Array.Empty<SqlExpression>(),
                keys.Select(k => new OrderingExpression(k, true)).ToArray());
            var query = source.QueryExpression;
            var alias = Alias();
            var lift = new ProjectionLifter(query, alias);
            var liftedIdentifier = (SqlExpression)lift.Visit(identifier);
            var collection = new CollectionShaperExpression(liftedIdentifier, lift.Visit(grouping.Element));
            var key = lift.Visit(grouping.Key);
            var constructor = typeof(ResultGrouping<,>).MakeGenericType(key.Type, grouping.Element.Type).GetConstructors().Single();

            query.Alias = alias;
            source.QueryExpression = new SelectExpression { From = query };
            source.QueryExpression.Orderings.Add(new OrderingExpression(liftedIdentifier, true));
            foreach (var ordering in grouping.Orderings)
                source.QueryExpression.Orderings.Add(new OrderingExpression((SqlExpression)lift.Visit(ordering.Expression), ordering.IsAscending));
            source.ShaperExpression = Expression.Convert(Expression.New(constructor, key, collection), grouping.Type);
            source.Collection = collection;
        }

        ShapedQueryExpression TranslateGroupJoin(MethodCallExpression method)
        {
            if (method.Arguments.Count != 5) throw Unsupported(method);
            var outer = Translate(method.Arguments[0]);
            var inner = Translate(method.Arguments[1]);
            PushdownIntoSubquery(outer);
            var outerWindow = new WindowFunctionExpression(WindowFunction.RowNumber, Array.Empty<SqlExpression>(),
                outer.QueryExpression.Orderings.ToArray());
            var outerRows = outer.QueryExpression;
            var alias = Alias();
            var lift = new ProjectionLifter(outerRows, alias);
            var identifier = (SqlExpression)lift.Visit(outerWindow);
            outer.ShaperExpression = lift.Visit(outer.ShaperExpression);
            outerRows.Orderings.Clear();
            outerRows.Alias = alias;
            outer.QueryExpression = new SelectExpression { From = outerRows };

            var presence = MakeOptional(inner, true);
            var predicate = JoinKeys(Bind(Lambda(method.Arguments[2]), outer.ShaperExpression),
                Bind(Lambda(method.Arguments[3]), inner.ShaperExpression));
            outer.QueryExpression.Joins.Add(new LeftJoinExpression(inner.QueryExpression.From, predicate));
            outer.QueryExpression.Orderings.Add(new OrderingExpression(identifier, true));
            outer.QueryExpression.Orderings.AddRange(inner.QueryExpression.Orderings);
            var collection = new CollectionShaperExpression(identifier, inner.ShaperExpression, presence);
            outer.Collection = collection;
            outer.ShaperExpression = Projection(Bind(Lambda(method.Arguments[4]), outer.ShaperExpression, collection));
            return outer;
        }

        bool TryTranslateGroupedElement(ShapedQueryExpression source, GroupByShaperExpression grouping, Expression projection)
        {
            var finder = new GroupedElementFindingVisitor(grouping);
            finder.Visit(projection);
            if (finder.Candidate == null) return false;
            var candidate = finder.Candidate;
            var isCollection = candidate.Method.Name == nameof(Enumerable.ToList);
            var input = candidate.Arguments[0];
            Expression count = null;
            if (isCollection)
            {
                if (!(input is MethodCallExpression take) || !IsSequenceMethod(take, nameof(Enumerable.Take))
                    || take.Arguments.Count != 2 || take.Arguments[1].Type != typeof(int))
                    throw Unsupported(candidate);
                count = take.Arguments[1];
                input = take.Arguments[0];
            }

            var orderings = ReadGroupOrderings(input, grouping);
            var keys = PrepareGroupingRows(source);
            var query = source.QueryExpression;
            var alias = Alias();
            var lift = new ProjectionLifter(query, alias);
            var element = lift.Visit(grouping.Element);
            var replacements = new Dictionary<Expression, Expression>();
            var liftedKey = new ProjectionLifter(query, alias, replacements).Visit(grouping.Key);
            replacements[grouping.Key] = liftedKey;
            var rank = (SqlExpression)lift.Visit(new WindowFunctionExpression(WindowFunction.RowNumber, keys, orderings));

            query.Alias = alias;
            var result = new SelectExpression { From = query };
            var replacement = element;
            if (isCollection)
            {
                var identifier = (SqlExpression)lift.Visit(new WindowFunctionExpression(WindowFunction.DenseRank, Array.Empty<SqlExpression>(),
                    keys.Select(k => new OrderingExpression(k, true)).ToArray()));
                var n = Scalar(count);
                var withinLimit = _sqlExpressionFactory.MakeBinary(ExpressionType.LessThanOrEqual, typeof(bool), rank, n);
                var presence = _sqlExpressionFactory.Case(typeof(int?), withinLimit,
                    new SqlConstantExpression(1, typeof(int?)), new SqlConstantExpression(null, typeof(int?)));
                result.Predicate = _sqlExpressionFactory.MakeBinary(ExpressionType.LessThanOrEqual, typeof(bool), rank,
                    _sqlExpressionFactory.Function("Greatest", typeof(int), Integer(1), n));
                result.Orderings.Add(new OrderingExpression(identifier, true));
                result.Orderings.Add(new OrderingExpression(rank, true));
                var collection = new CollectionShaperExpression(identifier, element, presence);
                source.Collection = collection;
                replacement = candidate.Update(null, new Expression[] { collection });
            }
            else result.Predicate = _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), rank, Integer(1));

            source.QueryExpression = result;
            source.ShaperExpression = Projection(new GroupedElementReplacingVisitor(candidate, replacement, grouping, replacements).Visit(projection));
            return true;
        }

        IReadOnlyList<OrderingExpression> ReadGroupOrderings(Expression input, GroupByShaperExpression grouping)
        {
            var orderings = new List<OrderingExpression>();
            while (input is MethodCallExpression order && order.Arguments.Count == 2)
            {
                var startsOrdering = IsSequenceMethod(order, nameof(Enumerable.OrderBy)) || IsSequenceMethod(order, nameof(Enumerable.OrderByDescending));
                var continuesOrdering = IsSequenceMethod(order, nameof(Enumerable.ThenBy)) || IsSequenceMethod(order, nameof(Enumerable.ThenByDescending));
                if (!startsOrdering && !continuesOrdering) break;
                orderings.Insert(0, new OrderingExpression(Scalar(Bind(Lambda(order.Arguments[1]), grouping.Element)),
                    !order.Method.Name.EndsWith("Descending", StringComparison.Ordinal)));
                input = order.Arguments[0];
                if (startsOrdering && ReferenceEquals(input, grouping))
                {
                    orderings.AddRange(grouping.Orderings);
                    return orderings;
                }
            }
            throw new NotSupportedException("Select grouped elements using an explicit OrderBy, with optional ThenBy clauses.");
        }

        static SqlExpression[] PrepareGroupingRows(ShapedQueryExpression source)
        {
            var query = source.QueryExpression;
            if (query.Having != null || query.IsDistinct || query.Offset != null || query.Limit != null || query.Orderings.Count != 0
                || source.ResultKind != QueryResultKind.Sequence)
                throw new NotSupportedException("Apply filters, ordering and paging to rows before projecting a grouped collection.");
            var keys = query.GroupBy.ToArray();
            query.GroupBy.Clear();
            return keys;
        }

        static bool IsSequenceMethod(MethodCallExpression method, string name)
            => (method.Method.DeclaringType == typeof(Enumerable) || method.Method.DeclaringType == typeof(Queryable)) && method.Method.Name == name;

        sealed class GroupedElementFindingVisitor : ExpressionVisitor
        {
            readonly GroupByShaperExpression _grouping;

            /// <summary>Creates a visitor that looks for terminal element selection on the supplied grouping.</summary>
            /// <param name="grouping">The grouping whose references are being translated.</param>
            internal GroupedElementFindingVisitor(GroupByShaperExpression grouping)
            {
                _grouping = grouping;
            }

            /// <summary>Gets the recognized grouped-element selector, if one was found.</summary>
            internal MethodCallExpression Candidate { get; private set; }

            /// <summary>Locates the supported terminal operation selecting elements from a group.</summary>
            /// <param name="node">The method call to visit.</param>
            /// <returns>The visited expression.</returns>
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Arguments.Count == 1 && (IsSequenceMethod(node, nameof(Enumerable.First))
                    || IsSequenceMethod(node, nameof(Enumerable.FirstOrDefault)) || IsSequenceMethod(node, nameof(Enumerable.ToList))))
                {
                    Expression root = node.Arguments[0];
                    while (root is MethodCallExpression chain && chain.Arguments.Count != 0)
                        root = chain.Arguments[0];
                    if (ReferenceEquals(root, _grouping))
                    {
                        if (Candidate != null) throw new NotSupportedException("Project only one ordered sequence of grouped elements per query.");
                        Candidate = node;
                        return node;
                    }
                }
                return base.VisitMethodCall(node);
            }
        }

        sealed class GroupedElementReplacingVisitor : ExpressionVisitor
        {
            readonly Expression _candidate;
            readonly Expression _replacement;
            readonly Expression _grouping;
            readonly IReadOnlyDictionary<Expression, Expression> _keys;

            /// <summary>Creates a visitor that replaces the grouped terminal and key references with projected expressions.</summary>
            /// <param name="candidate">The terminal grouped-element call to replace.</param>
            /// <param name="replacement">The expression replacing the original grouped-element terminal call.</param>
            /// <param name="grouping">The grouping whose references are being translated.</param>
            /// <param name="keys">The translated key expressions keyed by their original expression nodes.</param>
            internal GroupedElementReplacingVisitor(Expression candidate, Expression replacement, Expression grouping,
                IReadOnlyDictionary<Expression, Expression> keys)
            {
                _candidate = candidate;
                _replacement = replacement;
                _grouping = grouping;
                _keys = keys;
            }

            /// <summary>Replaces group keys and the ordered element selection with their translated projections.</summary>
            /// <param name="node">The expression to visit.</param>
            /// <returns>The visited expression.</returns>
            public override Expression Visit(Expression node)
            {
                if (ReferenceEquals(node, _candidate)) return _replacement;
                if (ReferenceEquals(node, _grouping))
                    throw new NotSupportedException("The ordered element projection cannot also read the group or evaluate a group aggregate.");
                return node != null && _keys.TryGetValue(node, out var key) ? key : base.Visit(node);
            }

            /// <summary>Rebinds members after replacing the ordered element selection.</summary>
            /// <param name="node">The member access to visit.</param>
            /// <returns>The visited expression.</returns>
            protected override Expression VisitMember(MemberExpression node)
            {
                // Resolve members such as g.OrderBy(...).First().Id after replacing the terminal.
                var visited = (MemberExpression)base.VisitMember(node);
                var parameter = Expression.Parameter(visited.Expression?.Type ?? typeof(object));
                return visited.Expression == null ? visited : Bind(Expression.Lambda(visited.Update(parameter), parameter), visited.Expression);
            }
        }
    }
}
