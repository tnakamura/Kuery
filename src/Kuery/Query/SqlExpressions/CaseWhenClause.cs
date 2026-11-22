namespace Kuery.Query.SqlExpressions
{
    /// <summary>Pairs a WHEN condition with its THEN result.</summary>
    internal sealed class CaseWhenClause
    {
        /// <summary>Creates a WHEN clause from its predicate and result.</summary>
        /// <param name="test">The SQL condition evaluated before choosing the corresponding value.</param>
        /// <param name="result">The SQL value returned when the clause matches.</param>
        internal CaseWhenClause(SqlExpression test, SqlExpression result)
        {
            Test = test;
            Result = result;
        }
        /// <summary>Gets the SQL predicate evaluated for this clause.</summary>
        internal SqlExpression Test { get; }
        /// <summary>Gets the value produced if the predicate matches.</summary>
        internal SqlExpression Result { get; }
        /// <summary>Replaces the condition and result of this clause.</summary>
        /// <param name="test">The SQL condition evaluated before choosing the corresponding value.</param>
        /// <param name="result">The SQL value returned when the clause matches.</param>
        /// <returns>This clause if unchanged, or the updated clause.</returns>
        internal CaseWhenClause Update(SqlExpression test, SqlExpression result)
            => ReferenceEquals(test, Test) && ReferenceEquals(result, Result) ? this : new CaseWhenClause(test, result);
    }
}
