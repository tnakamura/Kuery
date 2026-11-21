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
        /// <summary>Gets the enumerable CLR type of the materialized collection.</summary>
        public override Type Type => typeof(IEnumerable<>).MakeGenericType(Element.Type);
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }

    internal sealed class ResultGrouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        readonly IEnumerable<TElement> _elements;

        /// <summary>Initializes a materialized grouping with its key and elements.</summary>
        /// <param name="key">The shared key for the grouped values.</param>
        /// <param name="elements">The materialized values in the group.</param>
        public ResultGrouping(TKey key, IEnumerable<TElement> elements)
        {
            Key = key;
            _elements = elements;
        }

        /// <summary>Gets the key shared by the grouped elements.</summary>
        public TKey Key { get; }
        /// <summary>Returns an enumerator over the materialized group elements.</summary>
        /// <returns>An enumerator over the elements with this key.</returns>
        public IEnumerator<TElement> GetEnumerator() => _elements.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
