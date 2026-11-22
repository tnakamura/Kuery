using System;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Pairs a relational SELECT with its CLR result shape and cardinality.</summary>
    internal sealed class ShapedQueryExpression : Expression
    {
        /// <summary>Gets or sets the relational SELECT used to retrieve the result rows.</summary>
        internal SelectExpression QueryExpression { get; set; }
        /// <summary>Gets or sets the expression used to construct the CLR result.</summary>
        internal Expression ShaperExpression { get; set; }
        /// <summary>Gets or sets the collection shaper when one result spans multiple rows.</summary>
        /// <remarks>When present, the collection shaper coordinates adjacent rows before the final result is constructed.</remarks>
        internal CollectionShaperExpression Collection { get; set; }
        /// <summary>Gets or sets how execution should interpret the result rows.</summary>
        internal QueryResultKind ResultKind { get; set; }
        /// <summary>Gets the sequence or single-result cardinality implied by the result kind.</summary>
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
