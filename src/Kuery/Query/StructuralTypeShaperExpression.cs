using System;
using System.Linq.Expressions;

namespace Kuery.Query
{
    // Keeps materialization decisions separate from the entity's SQL projection.
    internal sealed class StructuralTypeShaperExpression : Expression
    {
        internal StructuralTypeShaperExpression(StructuralTypeProjectionExpression projection) { Projection = projection; }
        internal StructuralTypeProjectionExpression Projection { get; }
        /// <summary>Gets the CLR type of the entity being materialized.</summary>
        public override Type Type => Projection.Type;
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
