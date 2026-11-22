using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Kuery.Query;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery
{
    /// <summary>Creates and executes LINQ queries against a Kuery database connection.</summary>
    public sealed class KueryQueryProvider : IQueryProvider
    {
        readonly QueryContext _context;

        readonly IKueryLinqExecutor _executor;

        /// <summary>Gets the connection shared by query roots created by this provider.</summary>
        internal IDbConnection Connection => _context.Connection;

        /// <summary>Creates a LINQ provider using the default executor for the supplied connection.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        internal KueryQueryProvider(IDbConnection connection)
            : this(new QueryContext(connection), new KueryLinqExecutor())
        {
        }

        /// <summary>Creates a LINQ provider bound to a connection and an executor.</summary>
        /// <param name="context">The query execution context containing the database connection.</param>
        /// <param name="executor">The component that executes and materializes SQL commands.</param>
        internal KueryQueryProvider(QueryContext context, IKueryLinqExecutor executor)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        }

        /// <summary>Creates a deferred query for the specified expression tree.</summary>
        /// <param name="expression">The LINQ expression tree to translate.</param>
        /// <returns>A sequence that executes when enumerated.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="expression"/> is null.</exception>
        public IQueryable CreateQuery(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));

            var elementType = GetElementType(expression.Type);
            var queryType = typeof(KueryQueryable<>).MakeGenericType(elementType);
            return (IQueryable)Activator.CreateInstance(queryType, this, expression);
        }

        /// <summary>Creates a deferred query for the specified expression tree.</summary>
        /// <typeparam name="TElement">The element type of the query result.</typeparam>
        /// <param name="expression">The LINQ expression tree to translate.</param>
        /// <returns>A sequence that executes when enumerated.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="expression"/> is null.</exception>
        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return new KueryQueryable<TElement>(this, expression);
        }

        /// <summary>Translates and executes the specified expression against the database.</summary>
        /// <param name="expression">The LINQ expression tree to translate.</param>
        /// <returns>The materialized result of the expression.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="expression"/> is null.</exception>
        /// <exception cref="NotSupportedException">The expression cannot be translated to SQL.</exception>
        public object Execute(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return _executor.Execute(_context, expression, expression.Type);
        }

        /// <summary>Translates and executes the specified expression against the database.</summary>
        /// <typeparam name="TResult">The expected CLR result type.</typeparam>
        /// <param name="expression">The LINQ expression tree to translate.</param>
        /// <returns>The materialized result of the expression.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="expression"/> is null.</exception>
        /// <exception cref="NotSupportedException">The expression cannot be translated to SQL.</exception>
        public TResult Execute<TResult>(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return (TResult)_executor.Execute(_context, expression, typeof(TResult));
        }

        /// <summary>Executes a filtered bulk delete.</summary>
        /// <param name="expression">The expression to translate, inspect, or render.</param>
        /// <returns>The number of rows deleted.</returns>
        internal int ExecuteDelete(Expression expression)
        {
            Requires.NotNull(expression, nameof(expression));
            return _executor.ExecuteDelete(_context, expression);
        }

        /// <summary>Executes a filtered bulk delete asynchronously.</summary>
        /// <param name="expression">The expression to translate, inspect, or render.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the number of rows deleted.</returns>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        internal Task<int> ExecuteDeleteAsync(Expression expression, CancellationToken cancellationToken = default)
        {
            Requires.NotNull(expression, nameof(expression));
            return _executor.ExecuteDeleteAsync(_context, expression, cancellationToken);
        }

        /// <summary>Executes a filtered bulk update with the specified property assignments.</summary>
        /// <param name="expression">The expression to translate, inspect, or render.</param>
        /// <param name="setters">The property assignments for an update, if applicable.</param>
        /// <returns>The number of rows updated.</returns>
        internal int ExecuteUpdate(Expression expression, IReadOnlyList<SetPropertyCall> setters)
        {
            Requires.NotNull(expression, nameof(expression));
            Requires.NotNull(setters, nameof(setters));
            return _executor.ExecuteUpdate(_context, expression, setters);
        }

        /// <summary>Executes a filtered bulk update asynchronously with the specified property assignments.</summary>
        /// <param name="expression">The expression to translate, inspect, or render.</param>
        /// <param name="setters">The property assignments for an update, if applicable.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the number of rows updated.</returns>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        internal Task<int> ExecuteUpdateAsync(
            Expression expression,
            IReadOnlyList<SetPropertyCall> setters,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(expression, nameof(expression));
            Requires.NotNull(setters, nameof(setters));
            return _executor.ExecuteUpdateAsync(_context, expression, setters, cancellationToken);
        }

        /// <summary>Executes a LINQ expression asynchronously using the requested result type.</summary>
        /// <typeparam name="TResult">The expected CLR result type.</typeparam>
        /// <param name="expression">The expression to translate, inspect, or render.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the materialized query result.</returns>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        internal async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default)
        {
            Requires.NotNull(expression, nameof(expression));
            var result = await _executor.ExecuteAsync(_context, expression, typeof(TResult), cancellationToken).ConfigureAwait(false);
            return (TResult)result;
        }

        /// <summary>Compiles an expression and returns its SQL without running a command.</summary>
        /// <param name="expression">The expression to translate, inspect, or render.</param>
        /// <param name="resultType">The CLR type expected for the result.</param>
        /// <returns>The command text with parameter placeholders and any query tags.</returns>
        internal string GetQueryString(Expression expression, Type resultType)
            => new QueryCompiler().CompileQuery(_context, expression, resultType, false, out _).CommandText;

        /// <summary>Opens the result stream for a LINQ expression on asynchronous enumeration.</summary>
        /// <typeparam name="T">The mapped CLR type to read from the result.</typeparam>
        /// <param name="expression">The expression to translate, inspect, or render.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>An asynchronous sequence of materialized rows.</returns>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        internal IAsyncEnumerable<T> StreamAsync<T>(Expression expression, CancellationToken cancellationToken)
            => _executor.StreamAsync<T>(_context, expression, cancellationToken);

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
