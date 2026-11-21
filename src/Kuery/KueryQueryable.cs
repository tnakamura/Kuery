using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery
{
    /// <summary>Represents a deferred LINQ query that Kuery translates to SQL.</summary>
    /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
    public sealed class KueryQueryable<T> : IOrderedQueryable<T>
    {
        internal KueryQueryable(KueryQueryProvider provider)
        {
            Provider = provider;
            Expression = Expression.Constant(this);
        }

        internal KueryQueryable(KueryQueryProvider provider, Expression expression)
        {
            Provider = provider;
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        /// <summary>Gets the CLR type of the query results.</summary>
        public Type ElementType => typeof(T);

        /// <summary>Gets the expression tree that represents the query.</summary>
        public Expression Expression { get; }

        /// <summary>Gets the provider that translates and executes the query.</summary>
        public IQueryProvider Provider { get; }

        /// <summary>Executes the query and returns an enumerator over the materialized results.</summary>
        /// <returns>An enumerator over the query results.</returns>
        public IEnumerator<T> GetEnumerator()
        {
            var enumerable = Provider.Execute<IEnumerable<T>>(Expression);
            return enumerable.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
