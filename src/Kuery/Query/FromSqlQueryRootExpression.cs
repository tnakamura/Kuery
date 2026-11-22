using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query
{
    /// <summary>Represents a query root backed by caller-supplied SQL before relational translation.</summary>
    internal sealed class FromSqlQueryRootExpression : Expression
    {
        /// <summary>Creates a raw SQL query root before argument extraction.</summary>
        /// <param name="queryType">The queryable CLR type exposed by the SQL root.</param>
        /// <param name="source">The underlying mapped query root.</param>
        /// <param name="sql">The SQL command text or composable template.</param>
        /// <param name="arguments">The expressions supplying values for the positional placeholders.</param>
        internal FromSqlQueryRootExpression(Type queryType, Expression source, string sql, IReadOnlyList<Expression> arguments)
        {
            Type = queryType;
            Source = source;
            Sql = sql;
            Arguments = arguments;
        }

        /// <summary>Gets the queryable CLR type of the raw SQL source.</summary>
        public override Type Type { get; }
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Gets the underlying mapped query root.</summary>
        internal Expression Source { get; }
        /// <summary>Gets the composable SQL template containing positional argument placeholders.</summary>
        internal string Sql { get; }
        /// <summary>Gets the expressions supplying values for the SQL placeholders.</summary>
        internal IReadOnlyList<Expression> Arguments { get; }

        /// <summary>Visits the original query source and the SQL arguments.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => new FromSqlQueryRootExpression(Type, visitor.Visit(Source), Sql, Arguments.Select(visitor.Visit).ToArray());
    }
}
