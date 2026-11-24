using System;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    // Lower logical operators before null processing, without changing the cached SQL tree.
    /// <summary>Applies provider-specific changes to a translated SQL tree.</summary>
    internal sealed class RelationalQueryTranslationPostprocessor : SqlExpressionVisitor
    {
        readonly SqlDialectKind _dialect;
        /// <summary>Creates a SQL rewrite pass for the specified dialect.</summary>
        /// <param name="dialect">The provider SQL dialect for the operation.</param>
        internal RelationalQueryTranslationPostprocessor(SqlDialectKind dialect) { _dialect = dialect; }
        /// <summary>Rewrites the translated SQL tree for the current dialect.</summary>
        /// <param name="query">The query expression or SELECT to process.</param>
        /// <returns>The original query or its rewritten equivalent.</returns>
        /// <remarks>On SQLite, PostgreSQL, and MySQL, an isolated full join is rewritten as a left join unioned with unmatched right-hand rows using UNION ALL.</remarks>
        internal SelectExpression Process(SelectExpression query) => (SelectExpression)Visit(query);

        /// <summary>Rewrites full joins into equivalent set operations for dialects that require it.</summary>
        /// <param name="node">The SELECT statement to visit.</param>
        /// <returns>The visited expression.</returns>
        protected override Expression VisitSelect(SelectExpression node)
        {
            var query = (SelectExpression)base.VisitSelect(node);
            // PostgreSQL requires a hash- or merge-joinable condition for FULL JOIN, which null-compensated keys may lack.
            if (_dialect != SqlDialectKind.MySql && _dialect != SqlDialectKind.Sqlite && _dialect != SqlDialectKind.PostgreSql) return query;
            var full = query.Joins.Count == 1 ? query.Joins[0] as FullJoinExpression : null;
            if (full == null) return query;
            if (query.Predicate != null || query.Having != null || query.GroupBy.Count != 0 || query.Orderings.Count != 0
                || query.Limit != null || query.Offset != null || query.IsDistinct)
                throw new NotSupportedException("A FULL JOIN must be isolated before provider lowering.");
            // Translation isolates each FULL JOIN in its own SELECT, before subsequent composition.
            var left = new SelectExpression { From = query.From };
            left.Joins.Add(new LeftJoinExpression(full.Table, full.JoinPredicate));
            left.Projection.AddRange(query.Projection);
            var right = new SelectExpression { From = full.Table, Predicate = full.UnmatchedPredicate };
            right.Joins.Add(new LeftJoinExpression(query.From, full.JoinPredicate));
            right.Projection.AddRange(query.Projection);
            var alias = query.Alias + "_full";
            var result = new SelectExpression(query.Alias) { From = new UnionExpression(alias, left, right, false) };
            foreach (var projection in query.Projection)
                result.AddToProjection(new ColumnExpression(alias, projection.Alias, projection.Expression.Type, projection.Expression.IsNullable));
            return result;
        }
    }
}
