using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class InExpression : SqlExpression
    {
        internal InExpression(SqlExpression item, SqlExpression values) : base(typeof(bool), false)
        {
            Item = item;
            Values = values;
        }
        internal SqlExpression Item { get; }
        internal SqlExpression Values { get; }
        internal InExpression Update(SqlExpression item, SqlExpression values)
            => ReferenceEquals(item, Item) && ReferenceEquals(values, Values) ? this : new InExpression(item, values);
        /// <summary>Visits the item and the values used for membership testing.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update((SqlExpression)visitor.Visit(Item), (SqlExpression)visitor.Visit(Values));
    }
}
