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
        public override Type Type => typeof(IQueryable<>).MakeGenericType(Mapping.MappedType);
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
