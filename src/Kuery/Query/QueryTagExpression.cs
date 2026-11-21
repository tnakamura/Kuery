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

        /// <summary>Gets the CLR query type of the tagged source.</summary>
        public override Type Type => Source.Type;
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        internal Expression Source { get; }
        internal string Tag { get; }

        /// <summary>Visits the source query while preserving the tag text.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => new QueryTagExpression(visitor.Visit(Source), Tag);
    }
}
