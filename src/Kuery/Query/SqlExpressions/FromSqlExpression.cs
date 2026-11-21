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

        /// <summary>Visits the SQL arguments while preserving the command text.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var arguments = Arguments.Select(a => (SqlExpression)visitor.Visit(a)).ToArray();
            return Arguments.SequenceEqual(arguments) ? this : new FromSqlExpression(Alias, Sql, arguments);
        }
    }
}
