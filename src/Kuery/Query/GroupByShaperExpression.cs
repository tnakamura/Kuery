using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class GroupByShaperExpression : Expression
    {
        internal GroupByShaperExpression(Type type, Expression key, Expression element, IReadOnlyList<OrderingExpression> orderings = null)
        { Type = type; Key = key; Element = element; Orderings = orderings ?? Array.Empty<OrderingExpression>(); }
        /// <summary>Gets the CLR grouping type.</summary>
        public override Type Type { get; }
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal Expression Key { get; }
        internal Expression Element { get; }
        internal IReadOnlyList<OrderingExpression> Orderings { get; }
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
