using System;
using System.Linq.Expressions;

namespace Kuery.Query
{
    // Parameter extraction produces a query parameter; SQL translation chooses its SQL representation.
    /// <summary>References an extracted runtime value by slot before SQL translation.</summary>
    internal sealed class QueryParameterExpression : Expression
    {
        /// <summary>Creates a normalized reference to an extracted parameter slot.</summary>
        /// <param name="index">The zero-based runtime parameter slot.</param>
        /// <param name="type">The CLR type of the extracted query parameter.</param>
        internal QueryParameterExpression(int index, Type type)
        {
            Index = index;
            Type = type;
        }
        /// <summary>Gets the zero-based slot of the value in the runtime parameter list.</summary>
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
