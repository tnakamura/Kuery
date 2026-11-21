using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal enum WindowFunction
    {
        /// <summary>Numbers rows within each partition in window order.</summary>
        RowNumber,
        /// <summary>Assigns equal rank to ties without leaving gaps between ranks.</summary>
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

        /// <summary>Visits the partition keys and window orderings.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var partitions = Partitions.Select(p => (SqlExpression)visitor.Visit(p)).ToArray();
            var orderings = Orderings.Select(o => (OrderingExpression)visitor.Visit(o)).ToArray();
            return Partitions.SequenceEqual(partitions) && Orderings.SequenceEqual(orderings)
                ? this : new WindowFunctionExpression(Function, partitions, orderings);
        }
    }
}
