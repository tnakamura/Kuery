using System;
using System.Linq.Expressions;

namespace Kuery.Query
{
    // Keeps materialization decisions separate from the entity's SQL projection.
    internal sealed class StructuralTypeShaperExpression : Expression
    {
        internal StructuralTypeShaperExpression(StructuralTypeProjectionExpression projection) { Projection = projection; }
        internal StructuralTypeProjectionExpression Projection { get; }
        public override Type Type => Projection.Type;
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
