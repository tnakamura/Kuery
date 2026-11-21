using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    // Marks the collection to fill from adjacent rows with the same SQL identifier.
    internal sealed class CollectionShaperExpression : Expression
    {
        internal CollectionShaperExpression(SqlExpression identifier, Expression element, SqlExpression presence = null)
        {
            Identifier = identifier;
            Element = element;
            Presence = presence;
        }

        internal SqlExpression Identifier { get; }
        internal Expression Element { get; }
        internal SqlExpression Presence { get; }
        public override Type Type => typeof(IEnumerable<>).MakeGenericType(Element.Type);
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }

    internal sealed class ResultGrouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        readonly IEnumerable<TElement> _elements;

        public ResultGrouping(TKey key, IEnumerable<TElement> elements)
        {
            Key = key;
            _elements = elements;
        }

        public TKey Key { get; }
        public IEnumerator<TElement> GetEnumerator() => _elements.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
