using System;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents a typed literal value embedded in a SQL expression.</summary>
    internal sealed class SqlConstantExpression : SqlExpression
    {
        /// <summary>Creates a typed SQL literal for the supplied value.</summary>
        /// <param name="value">The value to render as a SQL literal, or null.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        internal SqlConstantExpression(object value, Type type) : base(type, value == null)
        {
            Value = value;
        }
        /// <summary>Gets the CLR value rendered as a SQL literal.</summary>
        internal object Value { get; }
    }
}
