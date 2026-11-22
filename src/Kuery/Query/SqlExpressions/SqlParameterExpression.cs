using System;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>References a SQL parameter through its runtime value slot.</summary>
    internal sealed class SqlParameterExpression : SqlExpression
    {
        /// <summary>Creates a SQL parameter expression for a runtime value slot.</summary>
        /// <param name="index">The zero-based runtime parameter slot.</param>
        /// <param name="type">The CLR type used for parameter binding.</param>
        internal SqlParameterExpression(int index, Type type)
            : base(type, !type.IsValueType || Nullable.GetUnderlyingType(type) != null) { Index = index; }
        /// <summary>Gets the zero-based slot used to bind the parameter value.</summary>
        internal int Index { get; }
    }
}
