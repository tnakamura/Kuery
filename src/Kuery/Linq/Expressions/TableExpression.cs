using System;
using System.Linq.Expressions;

namespace Kuery.Linq.Expressions
{
    internal class TableExpression : Expression
    {
        internal TableExpression(Type type, string alias, string name)
            : base()
        {
            NodeType = (ExpressionType)DbExpressionType.Table;
            Type = type;
            Alias = alias;
            Name = name;
        }

        /// <inheritdoc/>
        public override ExpressionType NodeType { get; }

        /// <inheritdoc/>
        public override Type Type { get; }

        internal string Alias { get; }

        internal string Name { get; }
    }
}
