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
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update((SqlExpression)visitor.Visit(Match), (SqlExpression)visitor.Visit(Pattern));
    }
}
