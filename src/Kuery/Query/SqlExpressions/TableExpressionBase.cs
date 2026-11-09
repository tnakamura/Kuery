using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal abstract class TableExpressionBase : Expression
    {
        protected TableExpressionBase(string alias)
        {
            Alias = alias;
        }
        internal string Alias { get; set; }
        public override Type Type => typeof(object);
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
