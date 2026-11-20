using System;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    // A missing row defaults the whole value, rather than constructing an object with NULL members.
    internal sealed class OptionalShaperExpression : Expression
    {
        internal OptionalShaperExpression(Expression value, SqlExpression presence)
        { Value = value; Presence = presence; }
        internal Expression Value { get; }
        internal SqlExpression Presence { get; }
        public override Type Type => Value.Type;
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var value = visitor.Visit(Value);
            var presence = (SqlExpression)visitor.Visit(Presence);
            return ReferenceEquals(value, Value) && ReferenceEquals(presence, Presence) ? this : new OptionalShaperExpression(value, presence);
        }
    }
}
