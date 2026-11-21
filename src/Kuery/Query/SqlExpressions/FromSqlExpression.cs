using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class FromSqlExpression : TableExpressionBase
    {
        internal FromSqlExpression(string alias, string sql, IReadOnlyList<SqlExpression> arguments) : base(alias)
        {
            Sql = sql;
            Arguments = arguments;
        }

        internal string Sql { get; }
        internal IReadOnlyList<SqlExpression> Arguments { get; }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var arguments = Arguments.Select(a => (SqlExpression)visitor.Visit(a)).ToArray();
            return Arguments.SequenceEqual(arguments) ? this : new FromSqlExpression(Alias, Sql, arguments);
        }
    }
}
