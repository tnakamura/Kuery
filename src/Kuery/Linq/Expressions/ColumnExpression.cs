using System;
using System.Linq.Expressions;

namespace Kuery.Linq.Expressions
{
    internal class ColumnExpression : Expression
    {
        internal ColumnExpression(Type type, string alias, string name, int ordinal)
            : base()
        {
            NodeType = (ExpressionType)DbExpressionType.Column;
            Type = type;
            Alias = alias;
            Name = name;
            Ordinal = ordinal;
        }

        /// <inheritdoc/>
        public override ExpressionType NodeType { get; }

        /// <inheritdoc/>
        public override Type Type { get; }

        internal string Alias { get; }

        internal string Name { get; }

        internal int Ordinal { get; }
    }
}
