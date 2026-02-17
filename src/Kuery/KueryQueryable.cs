using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery
{
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

        public Type ElementType => typeof(T);

        public Expression Expression { get; }

        public IQueryProvider Provider { get; }

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
