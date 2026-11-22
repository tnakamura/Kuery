using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    // Marks the collection to fill from adjacent rows with the same SQL identifier.
    /// <summary>Describes how adjacent rows form a projected collection and how to recognize a missing child.</summary>
    internal sealed class CollectionShaperExpression : Expression
    {
        /// <summary>Creates a collection shaper for the given SQL grouping identifier and element shape.</summary>
        /// <param name="identifier">The SQL identifier that determines which adjacent rows belong to the same group.</param>
        /// <param name="element">The expression used to materialize an element.</param>
        /// <param name="presence">The SQL marker whose null value signals the absence of a row, if applicable.</param>
        internal CollectionShaperExpression(SqlExpression identifier, Expression element, SqlExpression presence = null)
        {
            Identifier = identifier;
            Element = element;
            Presence = presence;
        }

        /// <summary>Gets the SQL value that identifies a group of adjacent rows.</summary>
        internal SqlExpression Identifier { get; }
        /// <summary>Gets the expression that materializes an element of the collection.</summary>
        internal Expression Element { get; }
        /// <summary>Gets the SQL value used to skip a missing child row, if needed.</summary>
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

    /// <summary>Represents a materialized LINQ grouping with a key and a sequence of elements.</summary>
    /// <typeparam name="TKey">The CLR type of the grouping key.</typeparam>
    /// <typeparam name="TElement">The CLR type of the group elements.</typeparam>
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
