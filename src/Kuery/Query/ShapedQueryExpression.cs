using System;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class ShapedQueryExpression : Expression
    {
        internal SelectExpression QueryExpression { get; set; }
        internal Expression ShaperExpression { get; set; }
        internal QueryResultKind ResultKind { get; set; }
        internal ResultCardinality ResultCardinality => ResultKind == QueryResultKind.Sequence
            ? ResultCardinality.Enumerable
            : ResultKind == QueryResultKind.FirstOrDefault || ResultKind == QueryResultKind.SingleOrDefault || ResultKind == QueryResultKind.ElementAtOrDefault
                ? ResultCardinality.SingleOrDefault : ResultCardinality.Single;

        public override ExpressionType NodeType => ExpressionType.Extension;
        public override Type Type => ResultCardinality == ResultCardinality.Enumerable
            ? typeof(IQueryable<>).MakeGenericType(ShaperExpression.Type) : ShaperExpression.Type;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
