using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Kuery.Query;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    /// <summary>Defines the execution operations used by the Kuery LINQ provider.</summary>
    internal interface IKueryLinqExecutor
    {
        /// <summary>Compiles and executes the query and materializes its result.</summary>
        /// <param name="context">The execution context containing the database connection.</param>
        /// <param name="expression">The expression to process.</param>
        /// <param name="resultType">The expected CLR result type of the query.</param>
        /// <returns>The materialized result.</returns>
        object Execute(QueryContext context, Expression expression, Type resultType);

        /// <summary>Compiles and executes the query and materializes its result asynchronously.</summary>
        /// <param name="context">The execution context containing the database connection.</param>
        /// <param name="expression">The expression to process.</param>
        /// <param name="resultType">The expected CLR result type of the query.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the materialized result.</returns>
        Task<object> ExecuteAsync(QueryContext context, Expression expression, Type resultType, CancellationToken cancellationToken);

        /// <summary>Streams the materialized query results asynchronously.</summary>
        /// <typeparam name="T">The element type of the query result.</typeparam>
        /// <param name="context">The execution context containing the database connection.</param>
        /// <param name="expression">The expression to process.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>The asynchronously enumerated query results.</returns>
        IAsyncEnumerable<T> StreamAsync<T>(QueryContext context, Expression expression, CancellationToken cancellationToken);

        /// <summary>Compiles and executes a set-based delete and returns the affected row count.</summary>
        /// <param name="context">The execution context containing the database connection.</param>
        /// <param name="expression">The expression to process.</param>
        /// <returns>The affected row count.</returns>
        int ExecuteDelete(QueryContext context, Expression expression);

        /// <summary>Executes a set-based delete asynchronously and returns the affected row count.</summary>
        /// <param name="context">The execution context containing the database connection.</param>
        /// <param name="expression">The expression to process.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the affected row count.</returns>
        Task<int> ExecuteDeleteAsync(QueryContext context, Expression expression, CancellationToken cancellationToken);

        /// <summary>Compiles and executes a set-based update using the supplied assignments.</summary>
        /// <param name="context">The execution context containing the database connection.</param>
        /// <param name="expression">The expression to process.</param>
        /// <param name="setters">The property assignments to apply to the selected rows.</param>
        /// <returns>The affected row count.</returns>
        int ExecuteUpdate(QueryContext context, Expression expression, IReadOnlyList<SetPropertyCall> setters);

        /// <summary>Executes a set-based update asynchronously using the supplied assignments.</summary>
        /// <param name="context">The execution context containing the database connection.</param>
        /// <param name="expression">The expression to process.</param>
        /// <param name="setters">The property assignments to apply to the selected rows.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the affected row count.</returns>
        Task<int> ExecuteUpdateAsync(QueryContext context, Expression expression, IReadOnlyList<SetPropertyCall> setters, CancellationToken cancellationToken);
    }
}
