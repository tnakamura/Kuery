using System;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class GroupByShaperExpression : Expression
    {
        internal GroupByShaperExpression(Type type, Expression key, Expression element)
        { Type = type; Key = key; Element = element; }
        public override Type Type { get; }
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal Expression Key { get; }
        internal Expression Element { get; }
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
