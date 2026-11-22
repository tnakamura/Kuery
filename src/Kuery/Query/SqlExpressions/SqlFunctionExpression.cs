using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents a SQL function or a logical operation rendered by a dialect.</summary>
    internal sealed class SqlFunctionExpression : SqlExpression
    {
        /// <summary>Creates a typed SQL function expression with its argument list.</summary>
        /// <param name="name">The logical name used to render the function.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <param name="nullable">Whether the SQL value can be NULL.</param>
        /// <param name="arguments">The SQL argument expressions in call order.</param>
        internal SqlFunctionExpression(string name, Type type, bool nullable, params SqlExpression[] arguments)
            : base(type, nullable)
        {
            Name = name;
            Arguments = arguments;
        }
        /// <summary>Gets the logical name used to resolve the function during SQL generation.</summary>
        internal string Name { get; }
        /// <summary>Gets the function arguments in call order.</summary>
        internal IReadOnlyList<SqlExpression> Arguments { get; }
        /// <summary>Replaces the function arguments while preserving its name and result metadata.</summary>
        /// <param name="arguments">The SQL argument expressions in call order.</param>
        /// <returns>This expression if unchanged, or the updated function call.</returns>
        internal SqlFunctionExpression Update(IReadOnlyList<SqlExpression> arguments) => Arguments.SequenceEqual(arguments) ? this
            : new SqlFunctionExpression(Name, Type, IsNullable, arguments.ToArray());
        /// <summary>Visits the SQL function arguments.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update(Arguments.Select(a => (SqlExpression)visitor.Visit(a)).ToArray());
    }
}
