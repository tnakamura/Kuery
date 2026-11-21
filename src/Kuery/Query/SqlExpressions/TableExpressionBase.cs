using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal abstract class TableExpressionBase : Expression
    {
        /// <summary>Initializes a table source with its SQL alias.</summary>
        /// <param name="alias">The SQL alias used to reference this source.</param>
        protected TableExpressionBase(string alias)
        {
            Alias = alias;
        }
        internal string Alias { get; set; }
        /// <summary>Gets the common CLR type used for a relational table source.</summary>
        public override Type Type => typeof(object);
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
