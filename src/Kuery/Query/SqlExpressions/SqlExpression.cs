using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal abstract class SqlExpression : Expression
    {
        protected SqlExpression(Type type, bool nullable)
        {
            Type = type;
            IsNullable = nullable;
        }

        public sealed override ExpressionType NodeType => ExpressionType.Extension;
        public sealed override Type Type { get; }
        internal bool IsNullable { get; }
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
