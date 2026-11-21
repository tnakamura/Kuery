using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kuery
{
    /// <summary>Executes Kuery LINQ queries asynchronously and streams their results.</summary>
    public static class QueryableAsyncExtensions
    {
        /// <summary>Streams the query results as they are read from the database.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to enumerate.</param>
        /// <param name="cancellationToken">A token that can cancel execution and database reads.</param>
        /// <returns>An asynchronous sequence that executes the query each time it is enumerated.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The source is not a Kuery query or the connection cannot stream asynchronously.</exception>
        /// <exception cref="OperationCanceledException">Enumeration was canceled.</exception>
        /// <remarks>Execution starts on the first read. Exhausting or disposing the enumerator releases the reader
        /// and command. The caller owns the connection and must keep it open while enumerating.</remarks>
        public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
            => GetProvider(source).StreamAsync<T>(source.Expression, cancellationToken);

        /// <summary>Tests asynchronously whether the query returns any rows.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is true if the query returns a row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<bool> AnyAsync<T>(this IQueryable<T> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<bool>(source, "Any", new[] { typeof(T) }, cancellationToken);

        /// <summary>Tests asynchronously whether any row satisfies a condition.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="predicate">The condition evaluated for each element.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is true if any row satisfies the condition.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<bool> AnyAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<bool>(source, "Any", new[] { typeof(T) }, cancellationToken, Quote(predicate, nameof(predicate)));

        /// <summary>Tests asynchronously whether every row satisfies a condition.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="predicate">The condition evaluated for each element.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is true if every row satisfies the condition, including when the query is empty.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<bool> AllAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<bool>(source, "All", new[] { typeof(T) }, cancellationToken, Quote(predicate, nameof(predicate)));

        /// <summary>Tests asynchronously whether the query contains the specified item.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="item">The value to find.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is true if the item occurs in the query result.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <remarks>Mapped entities are compared using their single primary key.</remarks>
        public static Task<bool> ContainsAsync<T>(this IQueryable<T> source, T item,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<bool>(source, "Contains", new[] { typeof(T) }, cancellationToken, TypedConstant(item));

        /// <summary>Counts the query results asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the number of rows as a 64-bit integer.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<long> LongCountAsync<T>(this IQueryable<T> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<long>(source, "LongCount", new[] { typeof(T) }, cancellationToken);

        /// <summary>Counts matching rows asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="predicate">The condition evaluated for each element.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the number of matching rows as a 64-bit integer.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<long> LongCountAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<long>(source, "LongCount", new[] { typeof(T) }, cancellationToken, Quote(predicate, nameof(predicate)));

        /// <summary>Returns the only row asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the only row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns zero or more than one matching row.</exception>
        public static Task<T> SingleAsync<T>(this IQueryable<T> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "Single", new[] { typeof(T) }, cancellationToken);

        /// <summary>Filters the query and returns the only row asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="predicate">The condition evaluated for each element.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the only row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns zero or more than one matching row.</exception>
        public static Task<T> SingleAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "Single", new[] { typeof(T) }, cancellationToken, Quote(predicate, nameof(predicate)));

        /// <summary>Returns the only row, or the default value when no row matches asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the only row, or the default value when no row matches.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns more than one matching row.</exception>
        public static Task<T> SingleOrDefaultAsync<T>(this IQueryable<T> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "SingleOrDefault", new[] { typeof(T) }, cancellationToken);

        /// <summary>Filters the query and returns the only row, or the default value when no row matches asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="predicate">The condition evaluated for each element.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the only row, or the default value when no row matches.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns more than one matching row.</exception>
        public static Task<T> SingleOrDefaultAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "SingleOrDefault", new[] { typeof(T) }, cancellationToken, Quote(predicate, nameof(predicate)));

        /// <summary>Returns the last row asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the last row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no matching rows.</exception>
        /// <remarks>An explicit ordering is required.</remarks>
        public static Task<T> LastAsync<T>(this IQueryable<T> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "Last", new[] { typeof(T) }, cancellationToken);

        /// <summary>Filters the query and returns the last row asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="predicate">The condition evaluated for each element.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the last row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no matching rows.</exception>
        /// <remarks>An explicit ordering is required.</remarks>
        public static Task<T> LastAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "Last", new[] { typeof(T) }, cancellationToken, Quote(predicate, nameof(predicate)));

        /// <summary>Returns the last row, or the default value when no row matches asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the last row, or the default value when no row matches.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <remarks>An explicit ordering is required.</remarks>
        public static Task<T> LastOrDefaultAsync<T>(this IQueryable<T> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "LastOrDefault", new[] { typeof(T) }, cancellationToken);

        /// <summary>Filters the query and returns the last row, or the default value when no row matches asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="predicate">The condition evaluated for each element.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the last row, or the default value when no row matches.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <remarks>An explicit ordering is required.</remarks>
        public static Task<T> LastOrDefaultAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "LastOrDefault", new[] { typeof(T) }, cancellationToken, Quote(predicate, nameof(predicate)));

        /// <summary>Returns the row at a zero-based index asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="index">The zero-based index of the row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the requested row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="ArgumentOutOfRangeException">The index is negative or outside the result.</exception>
        /// <remarks>Specify an explicit ordering when the selected row must be deterministic.</remarks>
        public static Task<T> ElementAtAsync<T>(this IQueryable<T> source, int index,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "ElementAt", new[] { typeof(T) }, cancellationToken, Expression.Constant(index));

        /// <summary>Returns the row at a zero-based index asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="index">The zero-based index of the row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the requested row, or the default value when the index is outside the result.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <remarks>Specify an explicit ordering when the selected row must be deterministic.</remarks>
        public static Task<T> ElementAtOrDefaultAsync<T>(this IQueryable<T> source, int index,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "ElementAtOrDefault", new[] { typeof(T) }, cancellationToken, Expression.Constant(index));

        /// <summary>Calculates the minimum asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the minimum, or null when no non-null value exists and the result type can represent null.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">No value exists and the result type cannot represent null.</exception>
        public static Task<T> MinAsync<T>(this IQueryable<T> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "Min", new[] { typeof(T) }, cancellationToken);

        /// <summary>Calculates the minimum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <typeparam name="TResult">The type of the value selected for comparison.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the minimum, or null when no non-null value exists and the result type can represent null.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">No value exists and the result type cannot represent null.</exception>
        public static Task<TResult> MinAsync<T, TResult>(this IQueryable<T> source, Expression<Func<T, TResult>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<TResult>(source, "Min", new[] { typeof(T), typeof(TResult) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Returns the row with the minimum key asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <typeparam name="TKey">The type of the selected key.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="keySelector">The expression that selects a comparable key from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the selected row, or null when the source is empty and the element type can represent null.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query is empty and the element type cannot represent null.</exception>
        /// <remarks>Non-null keys take precedence. An explicit source ordering resolves equal keys.</remarks>
        public static Task<T> MinByAsync<T, TKey>(this IQueryable<T> source, Expression<Func<T, TKey>> keySelector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "MinBy", new[] { typeof(T), typeof(TKey) }, cancellationToken, Quote(keySelector, nameof(keySelector)));

        /// <summary>Calculates the maximum asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the maximum, or null when no non-null value exists and the result type can represent null.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">No value exists and the result type cannot represent null.</exception>
        public static Task<T> MaxAsync<T>(this IQueryable<T> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "Max", new[] { typeof(T) }, cancellationToken);

        /// <summary>Calculates the maximum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <typeparam name="TResult">The type of the value selected for comparison.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the maximum, or null when no non-null value exists and the result type can represent null.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">No value exists and the result type cannot represent null.</exception>
        public static Task<TResult> MaxAsync<T, TResult>(this IQueryable<T> source, Expression<Func<T, TResult>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<TResult>(source, "Max", new[] { typeof(T), typeof(TResult) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Returns the row with the maximum key asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <typeparam name="TKey">The type of the selected key.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="keySelector">The expression that selects a comparable key from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the selected row, or null when the source is empty and the element type can represent null.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query is empty and the element type cannot represent null.</exception>
        /// <remarks>Non-null keys take precedence. An explicit source ordering resolves equal keys.</remarks>
        public static Task<T> MaxByAsync<T, TKey>(this IQueryable<T> source, Expression<Func<T, TKey>> keySelector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<T>(source, "MaxBy", new[] { typeof(T), typeof(TKey) }, cancellationToken, Quote(keySelector, nameof(keySelector)));

        /// <summary>Calculates the sum of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<int> SumAsync(this IQueryable<int> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<int>(source, "Sum", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the sum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<int> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, int>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<int>(source, "Sum", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the sum of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<int?> SumAsync(this IQueryable<int?> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<int?>(source, "Sum", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the sum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<int?> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, int?>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<int?>(source, "Sum", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the sum of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<long> SumAsync(this IQueryable<long> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<long>(source, "Sum", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the sum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<long> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, long>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<long>(source, "Sum", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the sum of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<long?> SumAsync(this IQueryable<long?> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<long?>(source, "Sum", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the sum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<long?> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, long?>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<long?>(source, "Sum", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the sum of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<float> SumAsync(this IQueryable<float> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<float>(source, "Sum", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the sum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<float> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, float>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<float>(source, "Sum", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the sum of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<float?> SumAsync(this IQueryable<float?> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<float?>(source, "Sum", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the sum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<float?> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, float?>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<float?>(source, "Sum", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the sum of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<double> SumAsync(this IQueryable<double> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double>(source, "Sum", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the sum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<double> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, double>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double>(source, "Sum", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the sum of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<double?> SumAsync(this IQueryable<double?> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double?>(source, "Sum", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the sum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<double?> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, double?>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double?>(source, "Sum", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the sum of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<decimal> SumAsync(this IQueryable<decimal> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<decimal>(source, "Sum", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the sum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<decimal> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, decimal>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<decimal>(source, "Sum", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the sum of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<decimal?> SumAsync(this IQueryable<decimal?> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<decimal?>(source, "Sum", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the sum of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the sum, or zero when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<decimal?> SumAsync<T>(this IQueryable<T> source, Expression<Func<T, decimal?>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<decimal?>(source, "Sum", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the average of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no rows.</exception>
        public static Task<double> AverageAsync(this IQueryable<int> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double>(source, "Average", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the average of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no rows.</exception>
        public static Task<double> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, int>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double>(source, "Average", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the average of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<double?> AverageAsync(this IQueryable<int?> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double?>(source, "Average", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the average of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<double?> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, int?>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double?>(source, "Average", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the average of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no rows.</exception>
        public static Task<double> AverageAsync(this IQueryable<long> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double>(source, "Average", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the average of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no rows.</exception>
        public static Task<double> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, long>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double>(source, "Average", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the average of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<double?> AverageAsync(this IQueryable<long?> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double?>(source, "Average", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the average of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<double?> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, long?>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double?>(source, "Average", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the average of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no rows.</exception>
        public static Task<float> AverageAsync(this IQueryable<float> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<float>(source, "Average", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the average of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no rows.</exception>
        public static Task<float> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, float>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<float>(source, "Average", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the average of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<float?> AverageAsync(this IQueryable<float?> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<float?>(source, "Average", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the average of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<float?> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, float?>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<float?>(source, "Average", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the average of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no rows.</exception>
        public static Task<double> AverageAsync(this IQueryable<double> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double>(source, "Average", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the average of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no rows.</exception>
        public static Task<double> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, double>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double>(source, "Average", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the average of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<double?> AverageAsync(this IQueryable<double?> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double?>(source, "Average", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the average of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<double?> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, double?>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<double?>(source, "Average", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the average of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no rows.</exception>
        public static Task<decimal> AverageAsync(this IQueryable<decimal> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<decimal>(source, "Average", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the average of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The query returns no rows.</exception>
        public static Task<decimal> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, decimal>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<decimal>(source, "Average", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Calculates the average of the query values asynchronously.</summary>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<decimal?> AverageAsync(this IQueryable<decimal?> source,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<decimal?>(source, "Average", Type.EmptyTypes, cancellationToken);

        /// <summary>Calculates the average of the projected values asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="selector">The expression that selects the numeric value from each row.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<decimal?> AverageAsync<T>(this IQueryable<T> source, Expression<Func<T, decimal?>> selector,
            CancellationToken cancellationToken = default)
            => ExecuteTerminalAsync<decimal?>(source, "Average", new[] { typeof(T) }, cancellationToken, Quote(selector, nameof(selector)));

        /// <summary>Reads the query results asynchronously into an array.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel execution and database reads.</param>
        /// <returns>A task whose result is an array containing the query results in their returned order.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static async Task<T[]> ToArrayAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
        {
            GetProvider(source);
            return (await source.ToListAsync(cancellationToken).ConfigureAwait(false)).ToArray();
        }

        /// <summary>Reads the query results asynchronously into a set.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel execution and database reads.</param>
        /// <returns>A task whose result is a set of the query results.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<HashSet<T>> ToHashSetAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken = default)
            => source.ToHashSetAsync(null, cancellationToken);

        /// <summary>Reads the query results asynchronously into a set.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="comparer">The client-side equality comparer, or null to use the default comparer.</param>
        /// <param name="cancellationToken">A token that can cancel execution and database reads.</param>
        /// <returns>A task whose result is a set of the query results.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static async Task<HashSet<T>> ToHashSetAsync<T>(this IQueryable<T> source,
            IEqualityComparer<T> comparer, CancellationToken cancellationToken = default)
        {
            var result = new HashSet<T>(comparer);
            await foreach (var item in source.AsAsyncEnumerable(cancellationToken).ConfigureAwait(false))
                result.Add(item);
            return result;
        }

        /// <summary>Reads the query results asynchronously into a dictionary keyed by the supplied selector.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="keySelector">The client-side function that selects a key for each result.</param>
        /// <param name="cancellationToken">A token that can cancel execution and database reads.</param>
        /// <returns>A task whose result is a dictionary of the query results.</returns>
        /// <exception cref="ArgumentNullException">A required argument or a selected key is null.</exception>
        /// <exception cref="ArgumentException">Two results have the same key.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(this IQueryable<T> source, Func<T, TKey> keySelector,
            CancellationToken cancellationToken = default)
            => source.ToDictionaryAsync(keySelector, comparer: null, cancellationToken: cancellationToken);

        /// <summary>Reads the query results asynchronously into a dictionary keyed by the supplied selector.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="keySelector">The client-side function that selects a key for each result.</param>
        /// <param name="comparer">The client-side key comparer, or null to use the default comparer.</param>
        /// <param name="cancellationToken">A token that can cancel execution and database reads.</param>
        /// <returns>A task whose result is a dictionary of the query results.</returns>
        /// <exception cref="ArgumentNullException">A required argument or a selected key is null.</exception>
        /// <exception cref="ArgumentException">Two results have the same key.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(this IQueryable<T> source, Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer, CancellationToken cancellationToken = default)
            => source.ToDictionaryAsync(keySelector, x => x, comparer, cancellationToken);

        /// <summary>Reads query results asynchronously and applies client-side key and value selectors.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <typeparam name="TResult">The type of the dictionary value.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="keySelector">The client-side function that selects a key for each result.</param>
        /// <param name="valueSelector">The client-side function that selects a value for each result.</param>
        /// <param name="cancellationToken">A token that can cancel execution and database reads.</param>
        /// <returns>A task whose result is a dictionary of the selected keys and values.</returns>
        /// <exception cref="ArgumentNullException">A required argument or a selected key is null.</exception>
        /// <exception cref="ArgumentException">Two results have the same key.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static Task<Dictionary<TKey, TResult>> ToDictionaryAsync<T, TKey, TResult>(this IQueryable<T> source,
            Func<T, TKey> keySelector, Func<T, TResult> valueSelector, CancellationToken cancellationToken = default)
            => source.ToDictionaryAsync(keySelector, valueSelector, null, cancellationToken);

        /// <summary>Reads query results asynchronously and applies client-side key and value selectors.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <typeparam name="TResult">The type of the dictionary value.</typeparam>
        /// <param name="source">The Kuery query to execute.</param>
        /// <param name="keySelector">The client-side function that selects a key for each result.</param>
        /// <param name="valueSelector">The client-side function that selects a value for each result.</param>
        /// <param name="comparer">The client-side key comparer, or null to use the default comparer.</param>
        /// <param name="cancellationToken">A token that can cancel execution and database reads.</param>
        /// <returns>A task whose result is a dictionary of the selected keys and values.</returns>
        /// <exception cref="ArgumentNullException">A required argument or a selected key is null.</exception>
        /// <exception cref="ArgumentException">Two results have the same key.</exception>
        /// <exception cref="NotSupportedException">The query provider or expression is unsupported.</exception>
        /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
        public static async Task<Dictionary<TKey, TResult>> ToDictionaryAsync<T, TKey, TResult>(this IQueryable<T> source,
            Func<T, TKey> keySelector, Func<T, TResult> valueSelector, IEqualityComparer<TKey> comparer,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(keySelector, nameof(keySelector));
            Requires.NotNull(valueSelector, nameof(valueSelector));
            var result = new Dictionary<TKey, TResult>(comparer);
            await foreach (var item in source.AsAsyncEnumerable(cancellationToken).ConfigureAwait(false))
                result.Add(keySelector(item), valueSelector(item));
            return result;
        }

        static KueryQueryProvider GetProvider(IQueryable source)
        {
            Requires.NotNull(source, nameof(source));
            return source.Provider as KueryQueryProvider ?? throw new NotSupportedException("This operation requires a Kuery query.");
        }

        static Expression Quote(LambdaExpression expression, string parameterName)
        {
            Requires.NotNull(expression, parameterName);
            return Expression.Quote(expression);
        }

        static Expression TypedConstant<T>(T item)
        {
            if (item != null && Nullable.GetUnderlyingType(typeof(T)) != null)
                return Expression.Convert(Expression.Constant(item), typeof(T));
            return Expression.Constant(item, typeof(T));
        }

        static Task<TResult> ExecuteTerminalAsync<TResult>(IQueryable source, string methodName, Type[] typeArguments,
            CancellationToken cancellationToken, params Expression[] arguments)
        {
            var provider = GetProvider(source);
            var expressions = new Expression[arguments.Length + 1];
            expressions[0] = source.Expression;
            Array.Copy(arguments, 0, expressions, 1, arguments.Length);
            var call = Expression.Call(typeof(Queryable), methodName, typeArguments, expressions);
            return provider.ExecuteAsync<TResult>(call, cancellationToken);
        }
    }
}
