using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents SQL membership testing against a collection of values.</summary>
    internal sealed class InExpression : SqlExpression
    {
        /// <summary>Creates a membership test for an item and a value collection.</summary>
        /// <param name="item">The SQL expression whose membership is tested.</param>
        /// <param name="values">The expression supplying the collection searched for the item.</param>
        internal InExpression(SqlExpression item, SqlExpression values) : base(typeof(bool), false)
        {
            Item = item;
            Values = values;
        }
        /// <summary>Gets the SQL expression for the item being tested.</summary>
        internal SqlExpression Item { get; }
        /// <summary>Gets the expression supplying the collection searched for a match.</summary>
        internal SqlExpression Values { get; }
        /// <summary>Replaces the tested item and its value collection.</summary>
        /// <param name="item">The SQL expression whose membership is tested.</param>
        /// <param name="values">The expression supplying the collection searched for the item.</param>
        /// <returns>This predicate if unchanged, or the updated membership test.</returns>
        internal InExpression Update(SqlExpression item, SqlExpression values)
            => ReferenceEquals(item, Item) && ReferenceEquals(values, Values) ? this : new InExpression(item, values);
        /// <summary>Visits the item and the values used for membership testing.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update((SqlExpression)visitor.Visit(Item), (SqlExpression)visitor.Visit(Values));
    }
}
