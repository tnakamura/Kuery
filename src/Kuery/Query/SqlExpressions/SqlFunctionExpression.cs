using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class SqlFunctionExpression : SqlExpression
    {
        internal SqlFunctionExpression(string name, Type type, bool nullable, params SqlExpression[] arguments)
            : base(type, nullable)
        {
            Name = name;
            Arguments = arguments;
        }
        internal string Name { get; }
        internal IReadOnlyList<SqlExpression> Arguments { get; }
        internal SqlFunctionExpression Update(IReadOnlyList<SqlExpression> arguments) => Arguments.SequenceEqual(arguments) ? this
            : new SqlFunctionExpression(Name, Type, IsNullable, arguments.ToArray());
        /// <summary>Visits the SQL function arguments.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update(Arguments.Select(a => (SqlExpression)visitor.Visit(a)).ToArray());
    }
}
