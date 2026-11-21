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
        public override Type Type { get; }
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal Expression Key { get; }
        internal Expression Element { get; }
        internal IReadOnlyList<OrderingExpression> Orderings { get; }
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
