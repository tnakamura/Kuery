using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Identifies the SQL window operations used for grouped materialization.</summary>
    internal enum WindowFunction
    {
        /// <summary>Numbers rows within each partition in window order.</summary>
        RowNumber,
        /// <summary>Assigns equal rank to ties without leaving gaps between ranks.</summary>
        DenseRank
    }

    /// <summary>Represents a SQL window function with optional partition keys and ordered rows.</summary>
    internal sealed class WindowFunctionExpression : SqlExpression
    {
        /// <summary>Creates a window function over the supplied partitions and sort order.</summary>
        /// <param name="function">The SQL window operation to invoke.</param>
        /// <param name="partitions">The SQL expressions that partition the window rows.</param>
        /// <param name="orderings">The SQL sort keys in priority order.</param>
        internal WindowFunctionExpression(WindowFunction function, IReadOnlyList<SqlExpression> partitions,
            IReadOnlyList<OrderingExpression> orderings) : base(typeof(long), false)
        {
            Function = function;
            Partitions = partitions;
            Orderings = orderings;
        }

        /// <summary>Gets the window operation to render.</summary>
        internal WindowFunction Function { get; }
        /// <summary>Gets the expressions that divide rows into independent partitions.</summary>
        internal IReadOnlyList<SqlExpression> Partitions { get; }
        /// <summary>Gets the sort keys used within the window.</summary>
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
