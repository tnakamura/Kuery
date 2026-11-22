using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Pairs a SQL sort key with its direction.</summary>
    internal sealed class OrderingExpression : Expression
    {
        /// <summary>Creates a SQL ordering over the supplied sort key.</summary>
        /// <param name="expression">The SQL sort key.</param>
        /// <param name="ascending">Whether to sort values in ascending order.</param>
        internal OrderingExpression(SqlExpression expression, bool ascending)
        { Expression = expression; IsAscending = ascending; }
        /// <summary>The SQL expression used as the sort key.</summary>
        internal SqlExpression Expression;
        /// <summary>Whether the sort key is ordered from lowest to highest.</summary>
        internal bool IsAscending;
        /// <summary>Gets the CLR type of the sort key.</summary>
        public override Type Type => Expression.Type;
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Replaces the sort key while preserving its direction.</summary>
        /// <param name="expression">The replacement SQL sort key.</param>
        /// <returns>This ordering if unchanged, or the updated ordering.</returns>
        internal OrderingExpression Update(SqlExpression expression)
            => ReferenceEquals(expression, Expression) ? this : new OrderingExpression(expression, IsAscending);
        /// <summary>Visits the sort key while preserving its direction.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SqlExpression)visitor.Visit(Expression));
    }
}
