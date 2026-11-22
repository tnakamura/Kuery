using System;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    // A missing row defaults the whole value, rather than constructing an object with NULL members.
    /// <summary>Represents a value that defaults when its source row is missing.</summary>
    internal sealed class OptionalShaperExpression : Expression
    {
        /// <summary>Creates a result shape that checks for a row before reading its value.</summary>
        /// <param name="value">The value used by the operation.</param>
        /// <param name="presence">The SQL marker whose null value signals the absence of a row, if applicable.</param>
        internal OptionalShaperExpression(Expression value, SqlExpression presence)
        { Value = value; Presence = presence; }
        /// <summary>Gets the CLR value shape to materialize when the row is present.</summary>
        internal Expression Value { get; }
        /// <summary>Gets the SQL marker whose null value indicates a missing row.</summary>
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
