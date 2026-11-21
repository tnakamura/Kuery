using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query
{
    internal sealed class FromSqlQueryRootExpression : Expression
    {
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
        internal Expression Source { get; }
        internal string Sql { get; }
        internal IReadOnlyList<Expression> Arguments { get; }

        /// <summary>Visits the original query source and the SQL arguments.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => new FromSqlQueryRootExpression(Type, visitor.Visit(Source), Sql, Arguments.Select(visitor.Visit).ToArray());
    }
}
