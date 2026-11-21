using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class ProjectionExpression : Expression
    {
        internal ProjectionExpression(SqlExpression expression, string alias)
        {
            Expression = expression;
            Alias = alias;
        }
        internal SqlExpression Expression { get; }
        internal string Alias { get; }
        /// <summary>Gets the CLR type of the projected expression.</summary>
        public override Type Type => Expression.Type;
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal ProjectionExpression Update(SqlExpression expression)
            => ReferenceEquals(expression, Expression) ? this : new ProjectionExpression(expression, Alias);
        /// <summary>Visits the projected value while preserving its alias.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SqlExpression)visitor.Visit(Expression));
    }
}
