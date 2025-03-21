using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Linq
{
    internal class Query<T> : IQueryable<T>
    {
        private readonly QueryProvider _provider;

        private readonly Expression _expression;

        internal Query(QueryProvider provider)
        {
            if (provider == null)
            {
                throw new ArgumentNullException(nameof(provider));
            }
            _provider = provider;
            _expression = Expression.Constant(this);
        }

        internal Query(QueryProvider provider, Expression expression)
        {
            if (provider == null)
            {
                throw new ArgumentNullException(nameof(provider));
            }
            if (expression == null)
            {
                throw new ArgumentNullException(nameof(expression));
            }
            if (typeof(IQueryable<T>).IsAssignableFrom(expression.Type) is false)
            {
                throw new ArgumentOutOfRangeException(nameof(expression));
            }
            _provider = provider;
            _expression = expression;
        }

        /// <inheritdoc/>
        Type IQueryable.ElementType => typeof(T);

        /// <inheritdoc/>
        Expression IQueryable.Expression => _expression;

        /// <inheritdoc/>
        IQueryProvider IQueryable.Provider => _provider;

        /// <inheritdoc/>
        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return ((IEnumerable<T>)_provider.Execute(_expression)).GetEnumerator();
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)_provider.Execute(_expression)).GetEnumerator();
        }
    }
}
