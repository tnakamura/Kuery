using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Pairs a projected SQL value with its output column alias.</summary>
    internal sealed class ProjectionExpression : Expression
    {
        /// <summary>Creates a projection for a SQL value and its output alias.</summary>
        /// <param name="expression">The SQL value to project.</param>
        /// <param name="alias">The SQL alias used to reference the table source or projected column.</param>
        internal ProjectionExpression(SqlExpression expression, string alias)
        {
            Expression = expression;
            Alias = alias;
        }
        /// <summary>Gets the SQL expression that supplies the projected value.</summary>
        internal SqlExpression Expression { get; }
        /// <summary>Gets the alias assigned to the projected column in the SELECT list.</summary>
        internal string Alias { get; }
        /// <summary>Gets the CLR type of the projected expression.</summary>
        public override Type Type => Expression.Type;
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Replaces the projected expression while retaining its alias.</summary>
        /// <param name="expression">The replacement SQL value.</param>
        /// <returns>This projection if unchanged, or the updated projection.</returns>
        internal ProjectionExpression Update(SqlExpression expression)
            => ReferenceEquals(expression, Expression) ? this : new ProjectionExpression(expression, Alias);
        /// <summary>Visits the projected value while preserving its alias.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SqlExpression)visitor.Visit(Expression));
    }
}
