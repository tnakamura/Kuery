using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Kuery.Linq;

namespace Kuery
{
    public sealed class KueryQueryProvider : IQueryProvider
    {
        readonly KueryQueryContext _context;

        readonly IKueryLinqExecutor _executor;

        internal KueryQueryProvider(IDbConnection connection)
            : this(new KueryQueryContext(connection), new KueryLinqExecutor())
        {
        }

        internal KueryQueryProvider(KueryQueryContext context, IKueryLinqExecutor executor)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        }

        public IQueryable CreateQuery(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));

            var elementType = GetElementType(expression.Type);
            var queryType = typeof(KueryQueryable<>).MakeGenericType(elementType);
            return (IQueryable)Activator.CreateInstance(queryType, this, expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return new KueryQueryable<TElement>(this, expression);
        }

        public object Execute(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return _executor.Execute(_context, expression, expression.Type);
        }

        public TResult Execute<TResult>(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return (TResult)_executor.Execute(_context, expression, typeof(TResult));
        }

        internal int ExecuteDelete(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return _executor.ExecuteDelete(_context, expression);
        }

        internal Task<int> ExecuteDeleteAsync(Expression expression, CancellationToken cancellationToken = default)
        {
            Requires.NotNull(expression, nameof(expression));
            return _executor.ExecuteDeleteAsync(_context, expression, cancellationToken);
        }

        internal int ExecuteUpdate(Expression expression, IReadOnlyList<SetPropertyCall> setters)
        {
            Requires.NotNull(expression, nameof(expression));
            Requires.NotNull(setters, nameof(setters));
            return _executor.ExecuteUpdate(_context, expression, setters);
        }

        internal Task<int> ExecuteUpdateAsync(
            Expression expression,
            IReadOnlyList<SetPropertyCall> setters,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(expression, nameof(expression));
            Requires.NotNull(setters, nameof(setters));
            return _executor.ExecuteUpdateAsync(_context, expression, setters, cancellationToken);
        }

        internal object BuildQueryModel(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return _executor.BuildQueryModel(_context, expression);
        }

        internal object ExecuteTerminal(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return _executor.ExecuteTerminal(_context, expression);
        }

        internal async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default)
        {
            Requires.NotNull(expression, nameof(expression));
            var result = await _executor.ExecuteAsync(_context, expression, typeof(TResult), cancellationToken).ConfigureAwait(false);
            return (TResult)result;
        }

        internal Task<object> ExecuteTerminalAsync(Expression expression, CancellationToken cancellationToken = default)
        {
            Requires.NotNull(expression, nameof(expression));
            return _executor.ExecuteTerminalAsync(_context, expression, cancellationToken);
        }

        static Type GetElementType(Type sequenceType)
        {
            if (sequenceType.IsGenericType && sequenceType.GetGenericTypeDefinition() == typeof(IQueryable<>))
            {
                return sequenceType.GetGenericArguments()[0];
            }

            var interfaceType = sequenceType.GetInterfaces()
                .FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IQueryable<>));

            return interfaceType?.GetGenericArguments()[0] ?? sequenceType;
        }
    }
}
