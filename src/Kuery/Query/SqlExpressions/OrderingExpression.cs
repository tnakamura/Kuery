using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class OrderingExpression : Expression
    {
        internal OrderingExpression(SqlExpression expression, bool ascending)
        { Expression = expression; IsAscending = ascending; }
        internal SqlExpression Expression;
        internal bool IsAscending;
        /// <summary>Gets the CLR type of the sort key.</summary>
        public override Type Type => Expression.Type;
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal OrderingExpression Update(SqlExpression expression)
            => ReferenceEquals(expression, Expression) ? this : new OrderingExpression(expression, IsAscending);
        /// <summary>Visits the sort key while preserving its direction.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SqlExpression)visitor.Visit(Expression));
    }
}
