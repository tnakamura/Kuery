using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kuery
{
    internal class Query<T> : IQueryable<T>, IOrderedQueryable<T>
    {
        private readonly QueryProvider provider;

        private readonly Expression expression;

        internal Query(QueryProvider provider)
        {
            if (provider == null)
            {
                throw new ArgumentNullException(nameof(provider));
            }

            this.provider = provider;
            expression = Expression.Constant(this);
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
            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentOutOfRangeException(nameof(expression));
            }

            this.provider = provider;
            this.expression = expression;
        }

        /// <inheritdoc/>
        public Type ElementType => typeof(T);

        /// <inheritdoc/>
        public Expression Expression => expression;

        /// <inheritdoc/>
        public IQueryProvider Provider => provider;

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator()
        {
            return ((IEnumerable<T>)provider.Execute<T>(expression)).GetEnumerator();
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)provider.Execute(expression)).GetEnumerator();
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return provider.GetQueryText(expression);
        }
    }

    internal abstract class QueryProvider : IQueryProvider
    {
        private protected QueryProvider()
        {
        }

        /// <inheritdoc/>
        public IQueryable CreateQuery(Expression expression)
        {
            var elementType = TypeSystem.GetElementType(expression.Type);
            var queryType = typeof(Query<>).MakeGenericType(elementType);
            try
            {
                return (IQueryable)Activator.CreateInstance(queryType, this, expression);
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        /// <inheritdoc/>
        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new Query<TElement>(this, expression);
        }

        /// <inheritdoc/>
        public abstract object Execute(Expression expression);

        /// <inheritdoc/>
        public TResult Execute<TResult>(Expression expression)
        {
            return (TResult)Execute(expression);
        }

        public abstract string GetQueryText(Expression expression);
    }

    internal class DbQueryProvider : QueryProvider
    {
        private readonly DbConnection connection;

        internal DbQueryProvider(DbConnection connection) : base()
        {
            this.connection = connection;
        }

        /// <inheritdoc/>
        public override object Execute(Expression expression)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public override string GetQueryText(Expression expression)
        {
            return expression.ToString();
        }
    }

    internal static class TypeSystem
    {
        internal static Type GetElementType(Type sequenceType)
        {
            var ienumerableType = FindIEnumerable(sequenceType);
            if (ienumerableType == null)
            {
                return sequenceType;
            }
            return ienumerableType.GetGenericArguments()[0];
        }

        private static Type FindIEnumerable(Type sequenceType)
        {
            if (sequenceType == null ||
                sequenceType == typeof(string))
            {
                return null;
            }

            if (sequenceType.IsArray)
            {
                return typeof(IEnumerable<>).MakeGenericType(sequenceType.GetElementType());
            }

            if (sequenceType.IsGenericType)
            {
                foreach (var arg in sequenceType.GetGenericArguments())
                {
                    var ienumerableType = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienumerableType.IsAssignableFrom(sequenceType))
                    {
                        return ienumerableType;
                    }
                }
            }

            var interfaceTypes = sequenceType.GetInterfaces();
            if (interfaceTypes != null && interfaceTypes.Length > 0)
            {
                foreach (var interfaceType in interfaceTypes)
                {
                    var ienumerableType = FindIEnumerable(interfaceType);
                    if (ienumerableType != null)
                    {
                        return ienumerableType;
                    }
                }
            }

            if (sequenceType.BaseType != null &&
                sequenceType.BaseType != typeof(object))
            {
                return FindIEnumerable(sequenceType.BaseType);
            }

            return null;
        }
    }
}
