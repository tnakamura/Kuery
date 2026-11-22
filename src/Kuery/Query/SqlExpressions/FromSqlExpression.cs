using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents composable SQL with positional arguments rendered as a derived table.</summary>
    internal sealed class FromSqlExpression : TableExpressionBase
    {
        /// <summary>Creates an aliased derived table from a SQL template and its arguments.</summary>
        /// <param name="alias">The SQL alias used to reference the table source or projected column.</param>
        /// <param name="sql">The SQL command text or composable template.</param>
        /// <param name="arguments">The expressions supplying values for the positional placeholders.</param>
        internal FromSqlExpression(string alias, string sql, IReadOnlyList<SqlExpression> arguments) : base(alias)
        {
            Sql = sql;
            Arguments = arguments;
        }

        /// <summary>Gets the composable SQL template used as the derived table.</summary>
        internal string Sql { get; }
        /// <summary>Gets the SQL expressions that replace the template placeholders.</summary>
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
