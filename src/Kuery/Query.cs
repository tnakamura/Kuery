using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Security;
using System.Text;

namespace Kuery
{
    public class Query<T> : IQueryable<T>,
        IQueryable,
        IEnumerable<T>,
        IEnumerable,
        IOrderedQueryable<T>,
        IOrderedQueryable
    {
        QueryProvider provider;

        Expression expression;

        public Query(QueryProvider provider)
        {
            Requires.NotNull(provider, nameof(provider));
            this.provider = provider;
            expression = Expression.Constant(this);
        }

        public Query(QueryProvider provider, Expression expression)
        {
            Requires.NotNull(provider, nameof(provider));
            Requires.NotNull(expression, nameof(expression));
            this.provider = provider;
            this.expression = expression;
        }

        /// <inheritdoc/>
        Expression IQueryable.Expression => expression;

        /// <inheritdoc/>
        Type IQueryable.ElementType => typeof(T);

        /// <inheritdoc/>
        IQueryProvider IQueryable.Provider => provider;

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator()
            => ((IEnumerable<T>)provider.Execute(expression)).GetEnumerator();

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
            => ((IEnumerable)provider.Execute(expression)).GetEnumerator();

        /// <inheritdoc/>
        public override string ToString()
            => provider.GetQueryText(expression);
    }

    public abstract class QueryProvider : IQueryProvider
    {
        protected QueryProvider() { }

        /// <inheritdoc/>
        IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
            => new Query<TElement>(this, expression);

        /// <inheritdoc/>
        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            var elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return (IQueryable)Activator.CreateInstance(
                    type: typeof(Query<>).MakeGenericType(elementType),
                    args: new object[] { this, expression });
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        /// <inheritdoc/>
        TResult IQueryProvider.Execute<TResult>(Expression expression)
            => (TResult)Execute(expression);

        /// <inheritdoc/>
        object IQueryProvider.Execute(Expression expression)
            => Execute(expression);

        public abstract string GetQueryText(Expression expression);

        public abstract object Execute(Expression expression);
    }

    internal static class TypeSystem
    {
        internal static Type GetElementType(Type seqType)
        {
            var ienum = FindIEnumerable(seqType);
            if (ienum == null)
            {
                return seqType;
            }
            else
            {
                return ienum.GetGenericArguments()[0];
            }
        }

        private static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
            {
                return null;
            }

            if (seqType.IsArray)
            {
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());
            }

            if (seqType.IsGenericType)
            {
                foreach (var arg in seqType.GetGenericArguments())
                {
                    var ienum = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienum.IsAssignableFrom(seqType))
                    {
                        return ienum;
                    }
                }
            }

            var ifaces = seqType.GetInterfaces();

            if (ifaces != null && ifaces.Length > 0)
            {
                foreach (var iface in ifaces)
                {
                    var ienum = FindIEnumerable(iface);
                    if (ienum != null)
                    {
                        return ienum;
                    }
                }
            }

            if (seqType.BaseType != null && seqType.BaseType != typeof(object))
            {
                return FindIEnumerable(seqType.BaseType);
            }

            return null;
        }
    }
}
