using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Kuery
{
    /// <remarks>Async connection extensions accept a cancellation token for database operations.</remarks>
    public static partial class SqlHelper
    {
        /// <summary>Inserts an entity using its mapped column values asynchronously.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="item">The entity whose mapped values are used by the operation.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows affected by the insert.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        /// <remarks>A generated primary key is assigned back to the entity when configured by the mapping.</remarks>
        public static async Task<int> InsertAsync<T>(
            this DbConnection connection,
            T item,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            return await connection.InsertAsync(typeof(T), item, transaction, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Inserts an entity using its mapped column values asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="item">The entity whose mapped values are used by the operation.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows affected by the insert.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> or <paramref name="type"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        /// <remarks>A generated primary key is assigned back to the entity when configured by the mapping.</remarks>
        public static async Task<int> InsertAsync(
            this DbConnection connection,
            Type type,
            object item,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(item, nameof(item));
            Requires.NotNull(type, nameof(type));

            var map = SqlMapper.GetMapping(type);
            if (map.PK != null &&
                map.PK.IsAutoGuid &&
                (Guid)map.PK.GetValue(item) == Guid.Empty)
            {
                map.PK.SetValue(item, Guid.NewGuid());
            }

            int count;
            using (var command = connection.CreateInsertCommand(item, type))
            {
                command.Transaction = transaction;
                count = await command.TryExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            if (map.HasAutoIncPK)
            {
                var id = await connection.GetLastRowIdAsync(transaction, cancellationToken).ConfigureAwait(false);
                map.SetAutoIncPk(item, id);
            }

            return count;
        }

        /// <summary>Executes a command asynchronously through the provider’s database command.</summary>
        /// <param name="command">The database command to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the affected row count returned by the provider.</returns>
        /// <exception cref="System.NotSupportedException">The command is not a DbCommand.</exception>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static async Task<int> TryExecuteNonQueryAsync(this IDbCommand command, CancellationToken cancellationToken = default)
        {
            if (command is DbCommand dbCommand)
            {
                return await dbCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                throw new NotSupportedException(
                    $"{nameof(command)} is not {nameof(DbCommand)}");
            }
        }

        /// <summary>Reads a scalar asynchronously through the provider’s database command.</summary>
        /// <param name="command">The database command to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the scalar returned by the provider.</returns>
        /// <exception cref="System.NotSupportedException">The command is not a DbCommand.</exception>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static async Task<object> TryExecuteScalarAsync(this IDbCommand command, CancellationToken cancellationToken = default)
        {
            if (command is DbCommand dbCommand)
            {
                return await dbCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                throw new NotSupportedException(
                    $"{nameof(command)} is not {nameof(DbCommand)}");
            }
        }

        private static async Task<long> GetLastRowIdAsync(
            this DbConnection connection,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            using (var command = connection.CreateLastInsertRowIdCommand())
            {
                command.Transaction = transaction;
                var result = await command.TryExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (result is decimal d)
                {
                    return (long)d;
                }
                return (long)result;
            }
        }

        /// <summary>Inserts each entity in the supplied sequence asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="items">The entities to process, in enumeration order.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the total number of rows affected by the inserts.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="items"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        /// <remarks>The runtime type of each entity determines its mapping. Supply a transaction to group the individual commands into one transaction.</remarks>
        public static async Task<int> InsertAllAsync(
            this DbConnection connection,
            IEnumerable items,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(items, nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                result += await connection.InsertAsync(Orm.GetType(item), item, transaction, cancellationToken).ConfigureAwait(false);
            }
            return result;
        }

        /// <summary>Inserts each entity in the supplied sequence asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="items">The entities to process, in enumeration order.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the total number of rows affected by the inserts.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="items"/> or <paramref name="type"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        /// <remarks>Supply a transaction to group the individual commands into one transaction.</remarks>
        public static async Task<int> InsertAllAsync(
            this DbConnection connection,
            Type type,
            IEnumerable items,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(items, nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                result += await connection.InsertAsync(type, item, transaction, cancellationToken).ConfigureAwait(false);
            }
            return result;
        }

        /// <summary>Updates the row identified by the entity’s primary key asynchronously.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="item">The entity whose mapped values are used by the operation.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows affected by the update.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<int> UpdateAsync<T>(
            this DbConnection connection,
            T item,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(item, nameof(item));

            return await connection.UpdateAsync(typeof(T), item, transaction, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Updates the row identified by the entity’s primary key asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="item">The entity whose mapped values are used by the operation.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows affected by the update.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> or <paramref name="type"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<int> UpdateAsync(
            this DbConnection connection, Type type,
            object item,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(item, nameof(item));
            Requires.NotNull(type, nameof(type));

            using (var command = connection.CreateUpdateCommand(item, type))
            {
                command.Transaction = transaction;
                return await command.TryExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Updates each supplied entity by its mapped primary key asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="items">The entities to process, in enumeration order.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the total number of rows affected by the updates.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="items"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        /// <remarks>The runtime type of each entity determines its mapping. Supply a transaction to group the individual commands into one transaction.</remarks>
        public static async Task<int> UpdateAllAsync(
            this DbConnection connection,
            IEnumerable items,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(items, nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                using (var command = connection.CreateUpdateCommand(item, Orm.GetType(item)))
                {
                    if (transaction != null)
                    {
                        command.Transaction = transaction;
                    }
                    result += await command.TryExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            return result;
        }


        /// <summary>Inserts or replaces a row using the database provider’s conflict handling asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="item">The entity to insert or replace. A null entity leaves the table unchanged.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows affected, or zero if the entity is null.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<int> InsertOrReplaceAsync(
            this DbConnection connection,
            object item,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            if (item == null)
            {
                return 0;
            }
            return await connection.InsertOrReplaceAsync(Orm.GetType(item), item, transaction, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Inserts or replaces a row using the database provider’s conflict handling asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="item">The entity to insert or replace. A null entity leaves the table unchanged.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows affected, or zero if the entity is null.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<int> InsertOrReplaceAsync(
            this DbConnection connection,
            Type type,
            object item,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(type, nameof(type));
            if (item == null)
            {
                return 0;
            }
            using (var command = connection.CreateInsertOrReplaceCommand(item, type))
            {
                command.Transaction = transaction;
                return await command.TryExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Deletes the row identified by the entity’s primary key asynchronously.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="item">The entity whose mapped values are used by the operation.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows deleted.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<int> DeleteAsync<T>(
            this DbConnection connection,
            T item,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(item, nameof(item));

            using (var command = connection.CreateDeleteCommand(item, typeof(T)))
            {
                command.Transaction = transaction;
                return await command.TryExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Deletes a row by its primary key asynchronously.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="primaryKey">The primary key value of the row to delete.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows deleted.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<int> DeleteAsync<T>(
            this DbConnection connection,
            object primaryKey,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            var map = SqlMapper.GetMapping<T>();
            return await connection.DeleteAsync(map, primaryKey, transaction, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Deletes a row by its primary key asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="primaryKey">The primary key value of the row to delete.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows deleted.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<int> DeleteAsync(
            this DbConnection connection,
            Type type,
            object primaryKey,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            var map = SqlMapper.GetMapping(type);
            return await connection.DeleteAsync(map, primaryKey, transaction, cancellationToken).ConfigureAwait(false);
        }

        private static async Task<int> DeleteAsync(
            this DbConnection connection,
            TableMapping map,
            object primaryKey,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(map, nameof(map));

            using (var command = connection.CreateDeleteCommand(primaryKey, map))
            {
                command.Transaction = transaction;
                return await command.TryExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Finds a row by primary key asynchronously.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="pk">The primary key value to look up.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the matching entity, or the default value if no row matches.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="pk"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<T> FindAsync<T>(
            this DbConnection connection,
            object pk,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(pk, nameof(pk));

            var map = SqlMapper.GetMapping(typeof(T));
            return await connection.FindAsync<T>(map, pk, cancellationToken).ConfigureAwait(false);
        }

        private static async Task<T> FindAsync<T>(
            this DbConnection connection,
            TableMapping mapping,
            object pk,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(pk, nameof(pk));
            Requires.NotNull(mapping, nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = await ExecuteQueryAsync<T>(command, mapping, cancellationToken).ConfigureAwait(false);
                return result.FirstOrDefault();
            }
        }

        /// <summary>Finds the first row matching a predicate asynchronously.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="predicate">The filter expression to evaluate against the mapped rows.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the matching entity, or the default value if no row matches.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<T> FindAsync<T>(
            this DbConnection connection,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            return await connection.Table<T>()
                .FirstOrDefaultAsync(predicate, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>Gets the first row matching a predicate asynchronously.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="predicate">The filter expression to evaluate against the mapped rows.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the matching entity.</returns>
        /// <exception cref="InvalidOperationException">No row matches the supplied key or predicate.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<T> GetAsync<T>(
            this DbConnection connection,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            return await connection.Table<T>()
                .FirstAsync(predicate, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>Gets the row with the specified primary key asynchronously.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="pk">The primary key value to look up.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the matching entity.</returns>
        /// <exception cref="InvalidOperationException">No row matches the supplied key or predicate.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="pk"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<T> GetAsync<T>(
            this DbConnection connection,
            object pk,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(pk, nameof(pk));

            var map = SqlMapper.GetMapping(typeof(T));
            return await connection.GetAsync<T>(map, pk, cancellationToken).ConfigureAwait(false);
        }

        private static async Task<T> GetAsync<T>(
            this DbConnection connection,
            TableMapping mapping,
            object pk,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(pk, nameof(pk));
            Requires.NotNull(mapping, nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = await ExecuteQueryAsync<T>(command, mapping, cancellationToken).ConfigureAwait(false);
                return result.First();
            }
        }

        /// <summary>Executes a DbCommand asynchronously and returns its result reader.</summary>
        /// <param name="command">The database command to execute.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the open reader for the command's result.</returns>
        /// <exception cref="System.NotSupportedException">The command is not a DbCommand.</exception>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        /// <remarks>The caller owns and must dispose the returned reader.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static async Task<DbDataReader> TryExecuteReaderAsync(this IDbCommand command, CancellationToken cancellationToken = default)
        {
            if (command is DbCommand dbCommand)
            {
                return await dbCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                throw new NotSupportedException(
                    $"{nameof(command)} is not {nameof(DbCommand)}");
            }
        }

        /// <summary>Executes a command and materializes the mapped rows asynchronously.</summary>
        /// <typeparam name="T">The mapped CLR type to read from the result.</typeparam>
        /// <param name="command">The database command to execute.</param>
        /// <param name="map">The mapping that associates CLR properties with database columns.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the materialized result rows.</returns>
        /// <exception cref="System.NotSupportedException">The command is not a DbCommand.</exception>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        internal static async Task<List<T>> ExecuteQueryAsync<T>(
            this IDbCommand command,
            TableMapping map,
            CancellationToken cancellationToken = default)
        {
            using (var reader = await command.TryExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                return await reader.ToListAsync<T>(map, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Executes a command and reads its first mapped row asynchronously.</summary>
        /// <typeparam name="T">The mapped CLR type to read from the result.</typeparam>
        /// <param name="command">The database command to execute.</param>
        /// <param name="map">The mapping that associates CLR properties with database columns.</param>
        /// <param name="cancellationToken">A token that can cancel database execution and reads.</param>
        /// <returns>A task whose result is the first mapped row, or the default value if the reader is empty.</returns>
        /// <exception cref="System.NotSupportedException">The command is not a DbCommand.</exception>
        /// <exception cref="System.OperationCanceledException">The database operation was canceled.</exception>
        internal static async Task<T> ExecuteQueryFirstOrDefaultAsync<T>(
            this IDbCommand command,
            TableMapping map,
            CancellationToken cancellationToken = default)
        {
            using (var reader = await command.TryExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                return await reader.FirstOrDefaultAsync<T>(map, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Executes a parameterized SQL query and maps its result columns asynchronously.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the materialized rows, in the order returned by the database.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<IEnumerable<T>> QueryAsync<T>(
            this DbConnection connection,
            string sql,
            object param = null,
            CancellationToken cancellationToken = default)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var map = SqlMapper.GetMapping<T>();
                return await ExecuteQueryAsync<T>(command, map, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Reads the query results asynchronously into a list.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="query">The query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel Kuery command execution and reads.</param>
        /// <returns>A task whose result is a list of rows in returned order.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> is null.</exception>
        /// <exception cref="NotSupportedException">A Kuery expression cannot be translated.</exception>
        /// <exception cref="OperationCanceledException">The Kuery operation was canceled.</exception>
        /// <remarks>For other LINQ providers, the existing synchronous execution behavior is used.</remarks>
        public static async Task<List<T>> ToListAsync<T>(
            this IQueryable<T> query,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));

            if (TryGetKueryProvider(query, out var provider))
            {
                return await provider.ExecuteAsync<List<T>>(query.Expression, cancellationToken).ConfigureAwait(false);
            }

            return query.ToList();
        }

        /// <summary>Counts the query results asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="query">The query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel Kuery command execution and reads.</param>
        /// <returns>A task whose result is the number of matching rows.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> is null.</exception>
        /// <exception cref="NotSupportedException">A Kuery expression cannot be translated.</exception>
        /// <exception cref="OperationCanceledException">The Kuery operation was canceled.</exception>
        /// <remarks>For other LINQ providers, the existing synchronous execution behavior is used.</remarks>
        public static async Task<int> CountAsync<T>(
            this IQueryable<T> query,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));

            if (TryGetKueryProvider(query, out var provider))
            {
                return await provider.ExecuteAsync<int>(CreateTerminalExpression(query, nameof(Queryable.Count)), cancellationToken).ConfigureAwait(false);
            }

            return query.Count();
        }

        /// <summary>Counts rows satisfying a condition asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="query">The query to execute.</param>
        /// <param name="predicate">The condition evaluated for each row.</param>
        /// <param name="cancellationToken">A token that can cancel Kuery command execution and reads.</param>
        /// <returns>A task whose result is the number of matching rows.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> or <paramref name="predicate"/> is null.</exception>
        /// <exception cref="NotSupportedException">A Kuery expression cannot be translated.</exception>
        /// <exception cref="OperationCanceledException">The Kuery operation was canceled.</exception>
        /// <remarks>For other LINQ providers, the existing synchronous execution behavior is used.</remarks>
        public static async Task<int> CountAsync<T>(
            this IQueryable<T> query,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));
            Requires.NotNull(predicate, nameof(predicate));

            return await query.Where(predicate).CountAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Returns the first row asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="query">The query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel Kuery command execution and reads.</param>
        /// <returns>A task whose result is the first row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> is null.</exception>
        /// <exception cref="NotSupportedException">A Kuery expression cannot be translated.</exception>
        /// <exception cref="OperationCanceledException">The Kuery operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">The result is empty.</exception>
        /// <remarks>For other LINQ providers, the existing synchronous execution behavior is used.</remarks>
        public static async Task<T> FirstAsync<T>(
            this IQueryable<T> query,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));

            if (TryGetKueryProvider(query, out var provider))
            {
                return await provider.ExecuteAsync<T>(CreateTerminalExpression(query, nameof(Queryable.First)), cancellationToken).ConfigureAwait(false);
            }

            return query.First();
        }

        /// <summary>Filters the query and returns its first row asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="query">The query to execute.</param>
        /// <param name="predicate">The condition evaluated for each row.</param>
        /// <param name="cancellationToken">A token that can cancel Kuery command execution and reads.</param>
        /// <returns>A task whose result is the first matching row.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> or <paramref name="predicate"/> is null.</exception>
        /// <exception cref="NotSupportedException">A Kuery expression cannot be translated.</exception>
        /// <exception cref="OperationCanceledException">The Kuery operation was canceled.</exception>
        /// <exception cref="InvalidOperationException">No row matches the condition.</exception>
        /// <remarks>For other LINQ providers, the existing synchronous execution behavior is used.</remarks>
        public static async Task<T> FirstAsync<T>(
            this IQueryable<T> query,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));
            Requires.NotNull(predicate, nameof(predicate));

            return await query.Where(predicate).FirstAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Returns the first row or a default value asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="query">The query to execute.</param>
        /// <param name="cancellationToken">A token that can cancel Kuery command execution and reads.</param>
        /// <returns>A task whose result is the first row, or the default value when the query is empty.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> is null.</exception>
        /// <exception cref="NotSupportedException">A Kuery expression cannot be translated.</exception>
        /// <exception cref="OperationCanceledException">The Kuery operation was canceled.</exception>
        /// <remarks>For other LINQ providers, the existing synchronous execution behavior is used.</remarks>
        public static async Task<T> FirstOrDefaultAsync<T>(
            this IQueryable<T> query,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));

            if (TryGetKueryProvider(query, out var provider))
            {
                return await provider.ExecuteAsync<T>(CreateTerminalExpression(query, nameof(Queryable.FirstOrDefault)), cancellationToken).ConfigureAwait(false);
            }

            return query.FirstOrDefault();
        }

        /// <summary>Returns the first matching row or a default value asynchronously.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="query">The query to execute.</param>
        /// <param name="predicate">The condition evaluated for each row.</param>
        /// <param name="cancellationToken">A token that can cancel Kuery command execution and reads.</param>
        /// <returns>A task whose result is the first matching row, or the default value if no row matches.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> or <paramref name="predicate"/> is null.</exception>
        /// <exception cref="NotSupportedException">A Kuery expression cannot be translated.</exception>
        /// <exception cref="OperationCanceledException">The Kuery operation was canceled.</exception>
        /// <remarks>For other LINQ providers, the existing synchronous execution behavior is used.</remarks>
        public static async Task<T> FirstOrDefaultAsync<T>(
            this IQueryable<T> query,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));
            Requires.NotNull(predicate, nameof(predicate));

            return await query.Where(predicate).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Deletes rows selected by a filtered Kuery query asynchronously.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="query">The Kuery query that selects the rows to modify.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows deleted.</returns>
        /// <exception cref="NotSupportedException">The provider or query shape does not support this operation.</exception>
        /// <exception cref="InvalidOperationException">No filter is specified.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        /// <remarks>The query must target a single mapped table with a filter.</remarks>
        public static async Task<int> ExecuteDeleteAsync<T>(
            this IQueryable<T> query,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));

            if (TryGetKueryProvider(query, out var provider))
            {
                return await provider.ExecuteDeleteAsync(query.Expression, cancellationToken).ConfigureAwait(false);
            }

            throw new NotSupportedException("ExecuteDeleteAsync is supported only for queries created by Kuery Query<T>().");
        }

        /// <summary>Updates the specified properties of rows selected by a filtered Kuery query asynchronously.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="query">The Kuery query that selects the rows to modify.</param>
        /// <param name="setPropertyCalls">A function that builds the property assignments for the update.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows updated.</returns>
        /// <exception cref="NotSupportedException">The provider or query shape does not support this operation.</exception>
        /// <exception cref="InvalidOperationException">No filter or property assignments are specified.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> or <paramref name="setPropertyCalls"/> is null.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        /// <remarks>The query must target a single mapped table with a filter.</remarks>
        public static async Task<int> ExecuteUpdateAsync<T>(
            this IQueryable<T> query,
            Func<SetPropertyCalls<T>, SetPropertyCalls<T>> setPropertyCalls,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));
            Requires.NotNull(setPropertyCalls, nameof(setPropertyCalls));

            var setters = setPropertyCalls(new SetPropertyCalls<T>());
            Requires.NotNull(setters, nameof(setPropertyCalls));

            if (TryGetKueryProvider(query, out var provider))
            {
                return await provider.ExecuteUpdateAsync(query.Expression, setters.Setters, cancellationToken).ConfigureAwait(false);
            }

            throw new NotSupportedException("ExecuteUpdateAsync is supported only for queries created by Kuery Query<T>().");
        }

        private static Expression CreateTerminalExpression<T>(IQueryable<T> query, string name)
        {
            return Expression.Call(typeof(Queryable), name, new[] { typeof(T) }, query.Expression);
        }

        private static bool TryGetKueryProvider<T>(IQueryable<T> query, out KueryQueryProvider provider)
        {
            provider = query.Provider as KueryQueryProvider;
            if (provider == null)
            {
                return false;
            }

            return true;
        }

        /// <summary>Executes a parameterized SQL query and maps its result columns asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the materialized rows, in the order returned by the database.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<IEnumerable<object>> QueryAsync(
            this DbConnection connection,
            Type type,
            string sql,
            object param = null,
            CancellationToken cancellationToken = default)
        {
            var map = SqlMapper.GetMapping(type);
            return await connection.QueryAsync(map, sql, param, cancellationToken).ConfigureAwait(false);
        }

        private static async Task<IEnumerable<object>> QueryAsync(
            this DbConnection connection,
            TableMapping mapping,
            string sql,
            object param = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(mapping, nameof(mapping));

            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return await ExecuteQueryAsync<object>(command, mapping, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Executes a parameterized SQL query and maps its first row asynchronously.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the first mapped result, or the default value if the query is empty.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<T> FindWithQueryAsync<T>(
            this DbConnection connection,
            string sql,
            object param = null,
            CancellationToken cancellationToken = default)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var map = SqlMapper.GetMapping<T>();
                return await ExecuteQueryFirstOrDefaultAsync<T>(command, map, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Executes a parameterized SQL query and maps its first row asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the first mapped result, or the default value if the query is empty.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<object> FindWithQueryAsync(
            this DbConnection connection,
            Type type,
            string sql,
            object param = null,
            CancellationToken cancellationToken = default)
        {
            var map = SqlMapper.GetMapping(type);
            return await connection.FindWithQueryAsync(map, sql, param, cancellationToken).ConfigureAwait(false);
        }

        private static async Task<object> FindWithQueryAsync(
            this DbConnection connection,
            TableMapping mapping,
            string sql,
            object param = null,
            CancellationToken cancellationToken = default)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return await ExecuteQueryFirstOrDefaultAsync<object>(command, mapping, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Executes a parameterized SQL command without reading a result set asynchronously.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the affected row count reported by the provider.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<int> ExecuteAsync(
            this DbConnection connection,
            string sql,
            object param = null,
            CancellationToken cancellationToken = default)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return await command.TryExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Executes a parameterized SQL command and reads its first column from the first row asynchronously.</summary>
        /// <typeparam name="T">The CLR type to which the scalar result is converted.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the scalar converted to the requested type, or the default value if the command returns no value or NULL.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public static async Task<T> ExecuteScalarAsync<T>(
            this DbConnection connection,
            string sql,
            object param = null,
            CancellationToken cancellationToken = default)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var result = await command.TryExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (result is null || result is DBNull)
                {
                    return default;
                }
                else
                {
                    return (T)Convert.ChangeType(result, typeof(T));
                }
            }
        }
    }

    /// <remarks>Async execution methods return materialized results.</remarks>
    public sealed partial class TableQuery<T>
    {
        /// <summary>Deletes rows matching the combined query conditions asynchronously.</summary>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows deleted.</returns>
        /// <exception cref="InvalidOperationException">The query has a row limit or offset, or no filter is specified.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public Task<int> DeleteAsync(CancellationToken cancellationToken = default) => DeleteAsync(null, cancellationToken);

        /// <summary>Deletes rows matching the combined query conditions asynchronously.</summary>
        /// <param name="predExpr">An optional filter to combine with the existing query conditions.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of rows deleted.</returns>
        /// <exception cref="InvalidOperationException">The query has a row limit or offset, or no filter is specified.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public async Task<int> DeleteAsync(Expression<Func<T, bool>> predExpr, CancellationToken cancellationToken = default)
        {
            if (_limit.HasValue || _offset.HasValue)
            {
                throw new InvalidOperationException("Cannot delete with limits or offsets");
            }
            if (_where == null && predExpr == null)
            {
                throw new InvalidOperationException("No condition specified");
            }

            var pred = _where;

            if (predExpr != null && predExpr.NodeType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)predExpr;
                pred = pred != null ? Expression.AndAlso(pred, lambda.Body) : lambda.Body;
            }

            var args = new List<object>();
            var cmdText = "delete from " + EscapeLiteral(Table.TableName);
            var w = CompileExpr(pred, args);
            cmdText += " where " + w.CommandText;

            using (var command = Connection.CreateCommand())
            {
                command.CommandText = cmdText;
                for (var i = 0; i < args.Count; i++)
                {
                    var a = args[i];
                    if (a == null)
                    {
                        continue;
                    }
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = GetParameterName("p" + (i + 1).ToString());
                    parameter.Value = a;
                    command.Parameters.Add(parameter);
                }
                return await command.TryExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Returns the row at the specified zero-based offset asynchronously.</summary>
        /// <param name="index">The zero-based row offset.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the row at the requested offset.</returns>
        /// <exception cref="InvalidOperationException">The query contains no row at the requested position.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public Task<T> ElementAtAsync(int index, CancellationToken cancellationToken = default)
        {
            return Skip(index).Take(1).FirstAsync(cancellationToken);
        }

        /// <summary>Executes the query and materializes the results in a list asynchronously.</summary>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is a list containing the materialized rows.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
        {
            return await GenerateCommand("*")
                .ExecuteQueryAsync<T>(Table, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>Executes the query and materializes the results in an array asynchronously.</summary>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>An array containing the materialized rows.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public async Task<T[]> ToArrayAsync(CancellationToken cancellationToken = default)
        {
            return (await ToListAsync(cancellationToken).ConfigureAwait(false)).ToArray();
        }

        /// <summary>Counts rows matching the query conditions asynchronously.</summary>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of matching rows.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public async Task<int> CountAsync(CancellationToken cancellationToken = default)
        {
            using (var command = GenerateCommand("count(*)"))
            {
                var result = await command.TryExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (result is long l)
                {
                    return (int)l;
                }
                return (int)result;
            }
        }

        /// <summary>Counts rows matching the query and the additional predicate asynchronously.</summary>
        /// <param name="predExpr">The additional filter to combine with the existing query conditions.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the number of matching rows.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public Task<int> CountAsync(Expression<Func<T, bool>> predExpr, CancellationToken cancellationToken = default)
        {
            return Where(predExpr).CountAsync(cancellationToken);
        }

        /// <summary>Returns the first row in the query result asynchronously.</summary>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the first matching row.</returns>
        /// <exception cref="InvalidOperationException">The query contains no row at the requested position.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public async Task<T> FirstAsync(CancellationToken cancellationToken = default)
        {
            var list = await Take(1).ToListAsync(cancellationToken);
            return list.First();
        }

        /// <summary>Returns the first matching row or its default value asynchronously.</summary>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the first matching row, or the default value if no row matches.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public async Task<T> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            var list = await Take(1).ToListAsync(cancellationToken);
            return list.FirstOrDefault();
        }

        /// <summary>Returns the first row matching the additional predicate asynchronously.</summary>
        /// <param name="predExpr">The additional filter to combine with the existing query conditions.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the first matching row.</returns>
        /// <exception cref="InvalidOperationException">The query contains no row at the requested position.</exception>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public Task<T> FirstAsync(Expression<Func<T, bool>> predExpr, CancellationToken cancellationToken = default)
        {
            return Where(predExpr).FirstAsync(cancellationToken);
        }

        /// <summary>Returns the first matching row or its default value asynchronously.</summary>
        /// <param name="predExpr">The additional filter to combine with the existing query conditions.</param>
        /// <param name="cancellationToken">A token that the database provider can observe to cancel the operation.</param>
        /// <returns>A task whose result is the first matching row, or the default value if no row matches.</returns>
        /// <exception cref="OperationCanceledException">The database operation was canceled.</exception>
        public Task<T> FirstOrDefaultAsync(Expression<Func<T, bool>> predExpr, CancellationToken cancellationToken = default)
        {
            return Where(predExpr).FirstOrDefaultAsync(cancellationToken);
        }
    }
}
