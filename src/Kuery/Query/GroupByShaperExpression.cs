using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Carries the group key, element projection, and input ordering during grouping translation.</summary>
    internal sealed class GroupByShaperExpression : Expression
    {
        /// <summary>Creates a grouping shape from its key, element, and input order.</summary>
        /// <param name="type">The CLR type of the grouping.</param>
        /// <param name="key">The projection that identifies a group.</param>
        /// <param name="element">The expression used to materialize an element.</param>
        /// <param name="orderings">The optional input ordering to preserve within each group.</param>
        internal GroupByShaperExpression(Type type, Expression key, Expression element, IReadOnlyList<OrderingExpression> orderings = null)
        { Type = type; Key = key; Element = element; Orderings = orderings ?? Array.Empty<OrderingExpression>(); }
        /// <summary>Gets the CLR grouping type.</summary>
        public override Type Type { get; }
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Gets the projection for the grouping key.</summary>
        internal Expression Key { get; }
        /// <summary>Gets the projection for one element of a group.</summary>
        internal Expression Element { get; }
        /// <summary>Gets the row orderings to preserve within groups.</summary>
        internal IReadOnlyList<OrderingExpression> Orderings { get; }
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
