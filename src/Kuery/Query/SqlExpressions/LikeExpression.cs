using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class LikeExpression : SqlExpression
    {
        internal LikeExpression(SqlExpression match, SqlExpression pattern) : base(typeof(bool), false)
        {
            Match = match;
            Pattern = pattern;
        }
        internal SqlExpression Match { get; }
        internal SqlExpression Pattern { get; }
        internal LikeExpression Update(SqlExpression match, SqlExpression pattern)
            => ReferenceEquals(match, Match) && ReferenceEquals(pattern, Pattern) ? this : new LikeExpression(match, pattern);
        /// <summary>Visits the match expression and pattern.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update((SqlExpression)visitor.Visit(Match), (SqlExpression)visitor.Visit(Pattern));
    }
}
