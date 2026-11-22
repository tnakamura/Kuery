using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Models a SELECT statement and its projection, sources, and clauses.</summary>
    internal sealed class SelectExpression : TableExpressionBase
    {
        /// <summary>Creates an empty SELECT with an optional alias for derived-table use.</summary>
        /// <param name="alias">The alias for a derived SELECT, or null for an unaliased statement.</param>
        internal SelectExpression(string alias = null) : base(alias)
        {
        }
        /// <summary>The primary table source, if this SELECT has a FROM clause.</summary>
        internal TableExpressionBase From;
        /// <summary>The joined sources in SQL clause order.</summary>
        internal readonly List<JoinExpressionBase> Joins = new List<JoinExpressionBase>();
        /// <summary>The projected values in result-column ordinal order.</summary>
        internal readonly List<ProjectionExpression> Projection = new List<ProjectionExpression>();

        /// <summary>Appends a SQL expression to the projection and assigns a generated output alias.</summary>
        /// <param name="expression">The SQL value to append to the SELECT list.</param>
        /// <returns>The zero-based result column ordinal.</returns>
        /// <remarks>The generated alias uses the returned ordinal. Callers can refer to it when lifting the projection.</remarks>
        internal int AddToProjection(SqlExpression expression)
        {
            var index = Projection.Count;
            Projection.Add(new ProjectionExpression(expression, "c" + index));
            return index;
        }

        /// <summary>The SQL expressions used as grouping keys.</summary>
        internal readonly List<SqlExpression> GroupBy = new List<SqlExpression>();
        /// <summary>The SQL sort keys in priority order.</summary>
        internal readonly List<OrderingExpression> Orderings = new List<OrderingExpression>();
        /// <summary>The condition used in the WHERE clause, if any.</summary>
        internal SqlExpression Predicate;
        /// <summary>The condition used in the HAVING clause, if any.</summary>
        internal SqlExpression Having;
        /// <summary>The SQL expression for the number of rows skipped.</summary>
        internal SqlExpression Offset;
        /// <summary>The SQL expression for the maximum number of rows returned.</summary>
        internal SqlExpression Limit;
        /// <summary>Whether the SELECT removes duplicate projected rows.</summary>
        internal bool IsDistinct;

        /// <summary>Visits the query sources, projections, and SQL clauses.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var from = (TableExpressionBase)visitor.Visit(From);
            var predicate = (SqlExpression)visitor.Visit(Predicate);
            var having = (SqlExpression)visitor.Visit(Having);
            var offset = (SqlExpression)visitor.Visit(Offset);
            var limit = (SqlExpression)visitor.Visit(Limit);
            var projections = Projection.Select(p => (ProjectionExpression)visitor.Visit(p)).ToArray();
            var groupBy = GroupBy.Select(p => (SqlExpression)visitor.Visit(p)).ToArray();
            var orderings = Orderings.Select(p => (OrderingExpression)visitor.Visit(p)).ToArray();
            var joins = Joins.Select(p => (JoinExpressionBase)visitor.Visit(p)).ToArray();
            if (ReferenceEquals(from, From) && ReferenceEquals(predicate, Predicate) && ReferenceEquals(having, Having)
                && ReferenceEquals(offset, Offset) && ReferenceEquals(limit, Limit) && Projection.SequenceEqual(projections)
                && GroupBy.SequenceEqual(groupBy) && Orderings.SequenceEqual(orderings) && Joins.SequenceEqual(joins)) return this;

            var updated = new SelectExpression(Alias)
            {
                From = from, Predicate = predicate, Having = having, Offset = offset, Limit = limit, IsDistinct = IsDistinct,
            };
            updated.Projection.AddRange(projections);
            updated.GroupBy.AddRange(groupBy);
            updated.Orderings.AddRange(orderings);
            updated.Joins.AddRange(joins);
            return updated;
        }
    }
}
