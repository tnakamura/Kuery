using System;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class QueryRootExpression : Expression
    {
        internal QueryRootExpression(TableMapping mapping)
        {
            Mapping = mapping;
        }
        internal TableMapping Mapping { get; }
        /// <summary>Gets the queryable CLR type of the mapped entity source.</summary>
        public override Type Type => typeof(IQueryable<>).MakeGenericType(Mapping.MappedType);
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
