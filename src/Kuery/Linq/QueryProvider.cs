using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kuery.Linq
{
    internal abstract class QueryProvider : IQueryProvider
    {
        protected QueryProvider()
        {
        }

        /// <inheritdoc/>
        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            var elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return (IQueryable)Activator.CreateInstance(
                    type: typeof(Query<>).MakeGenericType(elementType),
                    args: new object[]
                    {
                        this,
                        expression,
                    });
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        /// <inheritdoc/>
        IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
        {
            return new Query<TElement>(this, expression);
        }

        /// <inheritdoc/>
        object IQueryProvider.Execute(Expression expression)
        {
            return Execute(expression);
        }

        /// <inheritdoc/>
        TResult IQueryProvider.Execute<TResult>(Expression expression)
        {
            return (TResult)Execute(expression);
        }

        internal abstract string GetQueryText(Expression expression);

        internal abstract object Execute(Expression expression);
    }
}
