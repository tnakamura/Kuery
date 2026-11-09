using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class SelectExpression : TableExpressionBase
    {
        internal SelectExpression(string alias = null) : base(alias)
        {
        }
        internal TableExpressionBase From;
        internal readonly List<JoinExpressionBase> Joins = new List<JoinExpressionBase>();
        internal readonly List<ProjectionExpression> Projection = new List<ProjectionExpression>();

        internal int AddToProjection(SqlExpression expression)
        {
            var index = Projection.Count;
            Projection.Add(new ProjectionExpression(expression, "c" + index));
            return index;
        }

        internal readonly List<SqlExpression> GroupBy = new List<SqlExpression>();
        internal readonly List<OrderingExpression> Orderings = new List<OrderingExpression>();
        internal SqlExpression Predicate;
        internal SqlExpression Having;
        internal SqlExpression Offset;
        internal SqlExpression Limit;
        internal bool IsDistinct;

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
