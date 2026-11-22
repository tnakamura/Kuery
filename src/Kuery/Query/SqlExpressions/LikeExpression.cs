using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents string matching with a SQL LIKE pattern.</summary>
    internal sealed class LikeExpression : SqlExpression
    {
        /// <summary>Creates a LIKE predicate with a value and a pattern.</summary>
        /// <param name="match">The SQL string expression tested by LIKE.</param>
        /// <param name="pattern">The SQL expression supplying the LIKE pattern.</param>
        internal LikeExpression(SqlExpression match, SqlExpression pattern) : base(typeof(bool), false)
        {
            Match = match;
            Pattern = pattern;
        }
        /// <summary>Gets the SQL string expression to match against the pattern.</summary>
        internal SqlExpression Match { get; }
        /// <summary>Gets the SQL expression supplying the LIKE pattern.</summary>
        internal SqlExpression Pattern { get; }
        /// <summary>Replaces the matched value and pattern.</summary>
        /// <param name="match">The SQL string expression tested by LIKE.</param>
        /// <param name="pattern">The SQL expression supplying the LIKE pattern.</param>
        /// <returns>This predicate if unchanged, or the updated LIKE expression.</returns>
        internal LikeExpression Update(SqlExpression match, SqlExpression pattern)
            => ReferenceEquals(match, Match) && ReferenceEquals(pattern, Pattern) ? this : new LikeExpression(match, pattern);
        /// <summary>Visits the match expression and pattern.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update((SqlExpression)visitor.Visit(Match), (SqlExpression)visitor.Visit(Pattern));
    }
}
