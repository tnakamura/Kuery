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
        internal CollectionShaperExpression Collection { get; set; }
        internal QueryResultKind ResultKind { get; set; }
        internal ResultCardinality ResultCardinality => ResultKind == QueryResultKind.Sequence
            ? ResultCardinality.Enumerable
            : ResultKind == QueryResultKind.FirstOrDefault || ResultKind == QueryResultKind.SingleOrDefault || ResultKind == QueryResultKind.ElementAtOrDefault
                ? ResultCardinality.SingleOrDefault : ResultCardinality.Single;

        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Gets the CLR result type, taking the result cardinality into account.</summary>
        public override Type Type => ResultCardinality == ResultCardinality.Enumerable
            ? typeof(IQueryable<>).MakeGenericType(ShaperExpression.Type) : ShaperExpression.Type;
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
