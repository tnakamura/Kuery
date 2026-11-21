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
        /// <summary>Gets the CLR entity type represented by the identity parameters.</summary>
        public override Type Type { get; }
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Visits the key and presence expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => new EntityReferenceExpression(Type, visitor.Visit(Key), visitor.Visit(Present));
    }
}
