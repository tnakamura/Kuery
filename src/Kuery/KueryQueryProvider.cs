using System;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery
{
    public sealed class KueryQueryProvider : IQueryProvider
    {
        internal KueryQueryProvider(IDbConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        internal IDbConnection Connection { get; }

        public IQueryable CreateQuery(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));

            var elementType = expression.Type.GetGenericArguments()[0];
            var queryableType = typeof(KueryQueryable<>).MakeGenericType(elementType);
            return (IQueryable)Activator.CreateInstance(queryableType, this, expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return new KueryQueryable<TElement>(this, expression);
        }

        public object Execute(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            throw new NotSupportedException("KueryQueryProvider execution is not wired yet.");
        }

        public TResult Execute<TResult>(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            throw new NotSupportedException("KueryQueryProvider execution is not wired yet.");
        }
    }
}
