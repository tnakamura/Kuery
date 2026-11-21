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
        /// <summary>Gets the CLR type of the value being shaped.</summary>
        public override Type Type => Value.Type;
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Visits the value and its presence test.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var value = visitor.Visit(Value);
            var presence = (SqlExpression)visitor.Visit(Presence);
            return ReferenceEquals(value, Value) && ReferenceEquals(presence, Presence) ? this : new OptionalShaperExpression(value, presence);
        }
    }
}
