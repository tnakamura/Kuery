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
    public static partial class SqlHelper
    {
        public static async Task<int> InsertAsync<T>(
            this DbConnection connection,
            T item,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            return await connection.InsertAsync(typeof(T), item, transaction, cancellationToken).ConfigureAwait(false);
        }

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

        public static async Task<int> UpdateAsync<T>(
            this DbConnection connection,
            T item,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(item, nameof(item));

            return await connection.UpdateAsync(typeof(T), item, transaction, cancellationToken).ConfigureAwait(false);
        }

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

        public static async Task<int> DeleteAsync<T>(
            this DbConnection connection,
            object primaryKey,
            DbTransaction transaction = null,
            CancellationToken cancellationToken = default)
        {
            var map = SqlMapper.GetMapping<T>();
            return await connection.DeleteAsync(map, primaryKey, transaction, cancellationToken).ConfigureAwait(false);
        }

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

        public static async Task<T> FindAsync<T>(
            this DbConnection connection,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            return await connection.Table<T>()
                .FirstOrDefaultAsync(predicate, cancellationToken)
                .ConfigureAwait(false);
        }

        public static async Task<T> GetAsync<T>(
            this DbConnection connection,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            return await connection.Table<T>()
                .FirstAsync(predicate, cancellationToken)
                .ConfigureAwait(false);
        }

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

        public static async Task<List<T>> ToListAsync<T>(
            this IQueryable<T> query,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));

            if (TryGetTableQuery(query, out var tableQuery))
            {
                return await tableQuery.ToListAsync(cancellationToken).ConfigureAwait(false);
            }

            return query.ToList();
        }

        public static async Task<int> CountAsync<T>(
            this IQueryable<T> query,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));

            if (TryGetTableQuery(query, out var tableQuery))
            {
                return await tableQuery.CountAsync(cancellationToken).ConfigureAwait(false);
            }

            return query.Count();
        }

        public static async Task<int> CountAsync<T>(
            this IQueryable<T> query,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));
            Requires.NotNull(predicate, nameof(predicate));

            if (TryGetTableQuery(query, out var tableQuery))
            {
                return await tableQuery.Where(predicate).CountAsync(cancellationToken).ConfigureAwait(false);
            }

            return query.Count(predicate);
        }

        public static async Task<T> FirstAsync<T>(
            this IQueryable<T> query,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));

            if (TryGetTableQuery(query, out var tableQuery))
            {
                return await tableQuery.FirstAsync(cancellationToken).ConfigureAwait(false);
            }

            return query.First();
        }

        public static async Task<T> FirstAsync<T>(
            this IQueryable<T> query,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));
            Requires.NotNull(predicate, nameof(predicate));

            if (TryGetTableQuery(query, out var tableQuery))
            {
                return await tableQuery.Where(predicate).FirstAsync(cancellationToken).ConfigureAwait(false);
            }

            return query.First(predicate);
        }

        public static async Task<T> FirstOrDefaultAsync<T>(
            this IQueryable<T> query,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));

            if (TryGetTableQuery(query, out var tableQuery))
            {
                return await tableQuery.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            }

            return query.FirstOrDefault();
        }

        public static async Task<T> FirstOrDefaultAsync<T>(
            this IQueryable<T> query,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default)
        {
            Requires.NotNull(query, nameof(query));
            Requires.NotNull(predicate, nameof(predicate));

            if (TryGetTableQuery(query, out var tableQuery))
            {
                return await tableQuery.Where(predicate).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            }

            return query.FirstOrDefault(predicate);
        }

        private static bool TryGetTableQuery<T>(IQueryable<T> query, out TableQuery<T> tableQuery)
        {
            tableQuery = null;
            if (!(query.Provider is KueryQueryProvider provider))
            {
                return false;
            }

            tableQuery = provider.BuildTableQuery(query.Expression) as TableQuery<T>;
            return tableQuery != null;
        }

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

    public sealed partial class TableQuery<T>
    {
        public Task<int> DeleteAsync(CancellationToken cancellationToken = default) => DeleteAsync(null, cancellationToken);

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

        public Task<T> ElementAtAsync(int index, CancellationToken cancellationToken = default)
        {
            return Skip(index).Take(1).FirstAsync(cancellationToken);
        }

        public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
        {
            return await GenerateCommand("*")
                .ExecuteQueryAsync<T>(Table, cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task<T[]> ToArrayAsync(CancellationToken cancellationToken = default)
        {
            return (await ToListAsync(cancellationToken).ConfigureAwait(false)).ToArray();
        }

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

        public Task<int> CountAsync(Expression<Func<T, bool>> predExpr, CancellationToken cancellationToken = default)
        {
            return Where(predExpr).CountAsync(cancellationToken);
        }

        public async Task<T> FirstAsync(CancellationToken cancellationToken = default)
        {
            var list = await Take(1).ToListAsync(cancellationToken);
            return list.First();
        }

        public async Task<T> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            var list = await Take(1).ToListAsync(cancellationToken);
            return list.FirstOrDefault();
        }

        public Task<T> FirstAsync(Expression<Func<T, bool>> predExpr, CancellationToken cancellationToken = default)
        {
            return Where(predExpr).FirstAsync(cancellationToken);
        }

        public Task<T> FirstOrDefaultAsync(Expression<Func<T, bool>> predExpr, CancellationToken cancellationToken = default)
        {
            return Where(predExpr).FirstOrDefaultAsync(cancellationToken);
        }
    }
}

