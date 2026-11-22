using System;
using System.Linq.Expressions;

namespace Kuery.Query
{
    /// <summary>Associates a diagnostic tag with a query until parameter extraction removes it.</summary>
    internal sealed class QueryTagExpression : Expression
    {
        /// <summary>Wraps a query expression with diagnostic tag text.</summary>
        /// <param name="source">The source query expression.</param>
        /// <param name="tag">The diagnostic text to render as SQL comments.</param>
        internal QueryTagExpression(Expression source, string tag)
        {
            Source = source;
            Tag = tag;
        }

        /// <summary>Gets the CLR query type of the tagged source.</summary>
        public override Type Type => Source.Type;
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Gets the query expression to which the tag applies.</summary>
        internal Expression Source { get; }
        /// <summary>Gets the text to render as SQL comments for this execution.</summary>
        internal string Tag { get; }

        /// <summary>Visits the source query while preserving the tag text.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => new QueryTagExpression(visitor.Visit(Source), Tag);
    }
}
