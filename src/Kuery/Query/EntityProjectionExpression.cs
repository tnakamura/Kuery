using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class EntityProjectionExpression : Expression
    {
        internal EntityProjectionExpression(TableMapping mapping, IReadOnlyDictionary<string, Expression> members, SqlExpression presence = null)
        { Mapping = mapping; Members = members; Presence = presence; }
        internal TableMapping Mapping { get; }
        internal IReadOnlyDictionary<string, Expression> Members { get; }
        internal SqlExpression Presence { get; }
        public override Type Type => Mapping.MappedType;
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
