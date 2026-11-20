using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kuery
{
    /// <summary>Queryable operators unavailable in the .NET 10 standard API.</summary>
    public static class KueryQueryableExtensions
    {
        /// <summary>Returns matching pairs and unmatched rows from both Kuery queries.</summary>
        public static IQueryable<TResult> FullJoin<TOuter, TInner, TKey, TResult>(this IQueryable<TOuter> outer,
            IQueryable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, TInner, TResult>> resultSelector)
        {
            if (outer == null) throw new ArgumentNullException(nameof(outer));
            if (inner == null) throw new ArgumentNullException(nameof(inner));
            if (outerKeySelector == null) throw new ArgumentNullException(nameof(outerKeySelector));
            if (innerKeySelector == null) throw new ArgumentNullException(nameof(innerKeySelector));
            if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));
            var method = ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TOuter), typeof(TInner), typeof(TKey), typeof(TResult));
            return outer.Provider.CreateQuery<TResult>(Expression.Call(method, outer.Expression, inner.Expression,
                Expression.Quote(outerKeySelector), Expression.Quote(innerKeySelector), Expression.Quote(resultSelector)));
        }
    }
}
