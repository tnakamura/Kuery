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
        /// <summary>Gets the CLR type of the extracted parameter.</summary>
        public override Type Type { get; }
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
