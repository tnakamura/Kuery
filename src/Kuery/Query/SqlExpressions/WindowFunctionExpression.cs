using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal enum WindowFunction
    {
        RowNumber,
        DenseRank
    }

    internal sealed class WindowFunctionExpression : SqlExpression
    {
        internal WindowFunctionExpression(WindowFunction function, IReadOnlyList<SqlExpression> partitions,
            IReadOnlyList<OrderingExpression> orderings) : base(typeof(long), false)
        {
            Function = function;
            Partitions = partitions;
            Orderings = orderings;
        }

        internal WindowFunction Function { get; }
        internal IReadOnlyList<SqlExpression> Partitions { get; }
        internal IReadOnlyList<OrderingExpression> Orderings { get; }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var partitions = Partitions.Select(p => (SqlExpression)visitor.Visit(p)).ToArray();
            var orderings = Orderings.Select(o => (OrderingExpression)visitor.Visit(o)).ToArray();
            return Partitions.SequenceEqual(partitions) && Orderings.SequenceEqual(orderings)
                ? this : new WindowFunctionExpression(Function, partitions, orderings);
        }
    }
}
