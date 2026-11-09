namespace Kuery.Query.SqlExpressions
{
    internal sealed class CaseWhenClause
    {
        internal CaseWhenClause(SqlExpression test, SqlExpression result)
        {
            Test = test;
            Result = result;
        }
        internal SqlExpression Test { get; }
        internal SqlExpression Result { get; }
        internal CaseWhenClause Update(SqlExpression test, SqlExpression result)
            => ReferenceEquals(test, Test) && ReferenceEquals(result, Result) ? this : new CaseWhenClause(test, result);
    }
}
