using System;
using System.Linq.Expressions;

namespace Kuery.Query
{
    // Only scalar identity values survive parameter extraction; never retain the entity instance.
    internal sealed class EntityReferenceExpression : Expression
    {
        internal EntityReferenceExpression(Type type, Expression key, Expression present)
        { Type = type; Key = key; Present = present; }
        internal Expression Key { get; }
        internal Expression Present { get; }
        public override Type Type { get; }
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => new EntityReferenceExpression(Type, visitor.Visit(Key), visitor.Visit(Present));
    }
}
