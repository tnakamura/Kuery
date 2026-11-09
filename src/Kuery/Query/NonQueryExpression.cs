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
        public override Type Type => typeof(int);
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
