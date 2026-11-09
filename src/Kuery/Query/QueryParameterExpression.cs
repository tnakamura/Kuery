using System;
using System.Linq.Expressions;

namespace Kuery.Query
{
    // Parameter extraction produces a query parameter; SQL translation chooses its SQL representation.
    internal sealed class QueryParameterExpression : Expression
    {
        internal QueryParameterExpression(int index, Type type)
        {
            Index = index;
            Type = type;
        }
        internal int Index { get; }
        public override Type Type { get; }
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
