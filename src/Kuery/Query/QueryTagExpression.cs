using System;
using System.Linq.Expressions;

namespace Kuery.Query
{
    internal sealed class QueryTagExpression : Expression
    {
        internal QueryTagExpression(Expression source, string tag)
        {
            Source = source;
            Tag = tag;
        }

        public override Type Type => Source.Type;
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal Expression Source { get; }
        internal string Tag { get; }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => new QueryTagExpression(visitor.Visit(Source), Tag);
    }
}
