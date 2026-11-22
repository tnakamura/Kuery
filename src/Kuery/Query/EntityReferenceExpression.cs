using System;
using System.Linq.Expressions;

namespace Kuery.Query
{
    // Only scalar identity values survive parameter extraction; never retain the entity instance.
    /// <summary>Represents an entity in a comparison using key and presence expressions instead of the entity instance.</summary>
    internal sealed class EntityReferenceExpression : Expression
    {
        /// <summary>Creates an entity identity projection from its type, key, and presence.</summary>
        /// <param name="type">The CLR type represented by the identity expressions.</param>
        /// <param name="key">The expression supplying the mapped primary key value.</param>
        /// <param name="present">The expression indicating whether an entity instance was supplied.</param>
        internal EntityReferenceExpression(Type type, Expression key, Expression present)
        { Type = type; Key = key; Present = present; }
        /// <summary>Gets the expression that supplies the mapped primary key.</summary>
        internal Expression Key { get; }
        /// <summary>Gets the expression that indicates whether an entity value was supplied.</summary>
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
