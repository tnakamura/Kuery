using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class NonQueryExpression : Expression
    {
        internal SelectExpression QueryExpression { get; set; }
        internal string Operation { get; set; }
        internal TableMapping Table { get; set; }
        internal readonly List<KeyValuePair<string, SqlExpression>> Setters = new List<KeyValuePair<string, SqlExpression>>();
        /// <summary>Gets the affected row count type.</summary>
        public override Type Type => typeof(int);
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
