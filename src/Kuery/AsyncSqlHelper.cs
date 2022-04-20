using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;

namespace Kuery
{
    public static partial class SqlHelper
    {

        public static Task<int> InsertAsync<T>(this IDbConnection connection, T item, IDbTransaction transaction = null) =>
            connection.InsertAsync(typeof(T), item, transaction);

        public static async Task<int> InsertAsync(this IDbConnection connection, Type type, object item, IDbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (type == null) throw new ArgumentNullException(nameof(type));

            var map = connection.GetMapping(type);
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
                count = await command.TryExecuteNonQueryAsync();
            }

            if (map.HasAutoIncPK)
            {
                var id = await connection.GetLastRowIdAsync();
                map.SetAutoIncPk(item, id);
            }

            return count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Task<int> TryExecuteNonQueryAsync(this IDbCommand command)
        {
            if (command is DbCommand dbCommand)
            {
                return dbCommand.ExecuteNonQueryAsync();
            }
            else
            {
                throw new NotSupportedException(
                    $"{nameof(command)} is not {nameof(DbCommand)}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Task<object> TryExecuteScalarAsync(this IDbCommand command)
        {
            if (command is DbCommand dbCommand)
            {
                return dbCommand.ExecuteScalarAsync();
            }
            else
            {
                throw new NotSupportedException(
                    $"{nameof(command)} is not {nameof(DbCommand)}");
            }
        }

        private static async Task<long> GetLastRowIdAsync(this IDbConnection connection, IDbTransaction transaction = null)
        {
            using (var command = connection.CreateLastInsertRowIdCommand())
            {
                command.Transaction = transaction;
                return (long)await command.TryExecuteScalarAsync();
            }
        }

        public static async Task<int> InsertAllAsync(this IDbConnection connection, IEnumerable items, IDbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                result += await connection.InsertAsync(Orm.GetType(item), item, transaction);
            }
            return result;
        }

        public static async Task<int> InsertAllAsync(this IDbConnection connection, Type type, IEnumerable items, IDbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                result += await connection.InsertAsync(type, item, transaction);
            }
            return result;
        }

        public static Task<int> UpdateAsync<T>(this IDbConnection connection, T item, IDbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            return connection.UpdateAsync(typeof(T), item, transaction);
        }

        public static Task<int> UpdateAsync(this IDbConnection connection, Type type, object item, IDbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (type == null) throw new ArgumentNullException(nameof(type));

            using (var command = connection.CreateUpdateCommand(item, type))
            {
                command.Transaction = transaction;
                return command.TryExecuteNonQueryAsync();
            }
        }

        public static async Task<int> UpdateAllAsync(this IDbConnection connection, IEnumerable items, IDbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                using (var command = connection.CreateUpdateCommand(item, Orm.GetType(item)))
                {
                    if (transaction != null)
                    {
                        command.Transaction = transaction;
                    }
                    result += await command.TryExecuteNonQueryAsync();
                }
            }
            return result;
        }


        public static Task<int> InsertOrReplaceAsync(this IDbConnection connection, object item, IDbTransaction transaction = null)
        {
            if (item == null)
            {
                return Task.FromResult(0);
            }
            return connection.InsertOrReplaceAsync(Orm.GetType(item), item, transaction);
        }

        public static Task<int> InsertOrReplaceAsync(this IDbConnection connection, Type type, object item, IDbTransaction transaction = null)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }
            if (item == null)
            {
                return Task.FromResult(0);
            }
            using (var command = connection.CreateInsertOrReplaceCommand(item, type))
            {
                command.Transaction = transaction;
                return command.TryExecuteNonQueryAsync();
            }
        }

        public static Task<int> DeleteAsync<T>(this IDbConnection connection, T item, IDbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            using (var command = connection.CreateDeleteCommand(item, typeof(T)))
            {
                command.Transaction = transaction;
                return command.TryExecuteNonQueryAsync();
            }
        }

        public static Task<int> DeleteAsync<T>(this IDbConnection connection, object primaryKey, IDbTransaction transaction = null)
        {
            var map = connection.GetMapping<T>();
            return connection.DeleteAsync(map, primaryKey, transaction);
        }

        public static Task<int> DeleteAsync(this IDbConnection connection, Type type, object primaryKey, IDbTransaction transaction = null)
        {
            var map = connection.GetMapping(type);
            return connection.DeleteAsync(map, primaryKey, transaction);
        }

        private static Task<int> DeleteAsync(this IDbConnection connection, TableMapping map, object primaryKey, IDbTransaction transaction = null)
        {
            if (map == null) throw new ArgumentNullException(nameof(map));

            using (var command = connection.CreateDeleteCommand(primaryKey, map))
            {
                command.Transaction = transaction;
                return command.TryExecuteNonQueryAsync();
            }
        }

        public static Task<T> FindAsync<T>(this IDbConnection connection, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));

            var map = connection.GetMapping(typeof(T));
            return connection.FindAsync<T>(map, pk);
        }

        private static async Task<T> FindAsync<T>(this IDbConnection connection, TableMapping mapping, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = await ExecuteQueryAsync<T>(command, mapping);
                return result.FirstOrDefault();
            }
        }

        public static Task<T> FindAsync<T>(this IDbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().FirstOrDefaultAsync(predicate);
        }

        public static Task<T> GetAsync<T>(this IDbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().FirstAsync(predicate);
        }

        public static Task<T> GetAsync<T>(this IDbConnection connection, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));

            var map = connection.GetMapping(typeof(T));
            return connection.GetAsync<T>(map, pk);
        }

        private static async Task<T> GetAsync<T>(this IDbConnection connection, TableMapping mapping, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = await ExecuteQueryAsync<T>(command, mapping);
                return result.First();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Task<DbDataReader> TryExecuteReaderAsync(this IDbCommand command)
        {
            if (command is DbCommand dbCommand)
            {
                return dbCommand.ExecuteReaderAsync();
            }
            else
            {
                throw new NotSupportedException(
                    $"{nameof(command)} is not {nameof(DbCommand)}");
            }
        }

        internal static async Task<List<T>> ExecuteQueryAsync<T>(this IDbCommand command, TableMapping map)
        {
            using (var reader = await command.TryExecuteReaderAsync())
            {
                var result = new List<T>();
                var deserializer = new Deserializer<T>(map, reader);
                while (await reader.ReadAsync())
                {
                    var obj = deserializer.Deserialize(reader);
                    result.Add(obj);
                }
                return result;
            }
        }

        internal static async Task<T> ExecuteQueryFirstOrDefaultAsync<T>(this IDbCommand command, TableMapping map)
        {
            using (var reader = await command.TryExecuteReaderAsync().ConfigureAwait(false))
            {
                var deserializer = new Deserializer<T>(map, reader);
                if (await reader.ReadAsync().ConfigureAwait(false))
                {
                    return deserializer.Deserialize(reader);
                }
                else
                {
                    return default(T);
                }
            }
        }

        public static async Task<IEnumerable<T>> QueryAsync<T>(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var map = connection.GetMapping<T>();
                return await ExecuteQueryAsync<T>(command, map);
            }
        }

        public static Task<IEnumerable<object>> QueryAsync(this IDbConnection connection, Type type, string sql, object param = null)
        {
            var map = connection.GetMapping(type);
            return connection.QueryAsync(map, sql, param);
        }

        private static async Task<IEnumerable<object>> QueryAsync(this IDbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return await ExecuteQueryAsync<object>(command, mapping);
            }
        }

        public static Task<T> FindWithQueryAsync<T>(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var map = connection.GetMapping<T>();
                return ExecuteQueryFirstOrDefaultAsync<T>(command, map);
            }
        }

        public static Task<object> FindWithQueryAsync(this IDbConnection connection, Type type, string sql, object param = null)
        {
            var map = connection.GetMapping(type);
            return connection.FindWithQueryAsync(map, sql, param);
        }

        private static Task<object> FindWithQueryAsync(this IDbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQueryFirstOrDefaultAsync<object>(command, mapping);
            }
        }

        public static Task<int> ExecuteAsync(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return command.TryExecuteNonQueryAsync();
            }
        }

        public static async Task<T> ExecuteScalarAsync<T>(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var result = await command.TryExecuteScalarAsync();
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
}

