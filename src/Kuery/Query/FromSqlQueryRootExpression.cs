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

        public override Type Type { get; }
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal Expression Source { get; }
        internal string Sql { get; }
        internal IReadOnlyList<Expression> Arguments { get; }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => new FromSqlQueryRootExpression(Type, visitor.Visit(Source), Sql, Arguments.Select(visitor.Visit).ToArray());
    }
}
