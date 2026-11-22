using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Stores the target query and assignments for a bulk update or delete.</summary>
    internal sealed class NonQueryExpression : Expression
    {
        /// <summary>Gets or sets the SQL query selecting the rows to modify.</summary>
        internal SelectExpression QueryExpression { get; set; }
        /// <summary>Gets or sets the SQL operation kind for the mutation.</summary>
        internal string Operation { get; set; }
        /// <summary>Gets or sets the target table mapping.</summary>
        internal TableMapping Table { get; set; }
        /// <summary>The mapped column names and SQL expressions assigned by an update.</summary>
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
