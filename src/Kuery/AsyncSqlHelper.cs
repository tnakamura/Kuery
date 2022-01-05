using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Kuery
{
    public static partial class SqlHelper
    {

        public static Task<int> InsertAsync<T>(this DbConnection connection, T item, DbTransaction transaction = null) =>
            connection.InsertAsync(typeof(T), item, transaction);

        public static async Task<int> InsertAsync(this DbConnection connection, Type type, object item, DbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (type == null) throw new ArgumentNullException(nameof(type));

            var map = GetMapping(type);
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
                count = await command.ExecuteNonQueryAsync();
            }

            if (map.HasAutoIncPK)
            {
                var id = await connection.GetLastRowIdAsync();
                map.SetAutoIncPk(item, id);
            }

            return count;
        }

        private static async Task<long> GetLastRowIdAsync(this DbConnection connection, DbTransaction transaction = null)
        {
            using (var command = connection.CreateLastInsertRowIdCommand())
            {
                command.Transaction = transaction;
                return (long)await command.ExecuteScalarAsync();
            }
        }

        public static async Task<int> InsertAllAsync(this DbConnection connection, IEnumerable items, DbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                result += await connection.InsertAsync(Orm.GetType(item), item, transaction);
            }
            return result;
        }

        public static async Task<int> InsertAllAsync(this DbConnection connection, Type type, IEnumerable items, DbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                result += await connection.InsertAsync(type, item, transaction);
            }
            return result;
        }

        public static Task<int> UpdateAsync<T>(this DbConnection connection, T item, DbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            return connection.UpdateAsync(typeof(T), item, transaction);
        }

        public static Task<int> UpdateAsync(this DbConnection connection, Type type, object item, DbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (type == null) throw new ArgumentNullException(nameof(type));

            using (var command = connection.CreateUpdateCommand(item, type))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQueryAsync();
            }
        }

        public static async Task<int> UpdateAllAsync(this DbConnection connection, IEnumerable items, DbTransaction transaction = null)
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
                    result += await command.ExecuteNonQueryAsync();
                }
            }
            return result;
        }


        public static Task<int> InsertOrReplaceAsync(this DbConnection connection, object item, DbTransaction transaction = null)
        {
            if (item == null)
            {
                return Task.FromResult(0);
            }
            return connection.InsertOrReplaceAsync(Orm.GetType(item), item, transaction);
        }

        public static Task<int> InsertOrReplaceAsync(this DbConnection connection, Type type, object item, DbTransaction transaction = null)
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
                return command.ExecuteNonQueryAsync();
            }
        }

        public static Task<int> DeleteAsync<T>(this DbConnection connection, T item, DbTransaction transaction = null)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            using (var command = connection.CreateDeleteCommand(item, typeof(T)))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQueryAsync();
            }
        }

        public static Task<int> DeleteAsync<T>(this DbConnection connection, object primaryKey, DbTransaction transaction = null) =>
            connection.DeleteAsync(GetMapping<T>(), primaryKey, transaction);

        public static Task<int> DeleteAsync(this DbConnection connection, Type type, object primaryKey, DbTransaction transaction = null) =>
            connection.DeleteAsync(GetMapping(type), primaryKey, transaction);

        private static Task<int> DeleteAsync(this DbConnection connection, TableMapping map, object primaryKey, DbTransaction transaction = null)
        {
            if (map == null) throw new ArgumentNullException(nameof(map));

            using (var command = connection.CreateDeleteCommand(primaryKey, map))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQueryAsync();
            }
        }

        public static Task<T> FindAsync<T>(this DbConnection connection, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));

            var map = GetMapping(typeof(T));
            return connection.FindAsync<T>(map, pk);
        }

        private static async Task<T> FindAsync<T>(this DbConnection connection, TableMapping mapping, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = await ExecuteQueryAsync<T>(command, mapping);
                return result.FirstOrDefault();
            }
        }

        public static Task<T> FindAsync<T>(this DbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().FirstOrDefaultAsync(predicate);
        }

        public static Task<T> GetAsync<T>(this DbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().FirstAsync(predicate);
        }

        public static Task<T> GetAsync<T>(this DbConnection connection, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));

            var map = GetMapping(typeof(T));
            return connection.GetAsync<T>(map, pk);
        }

        private static async Task<T> GetAsync<T>(this DbConnection connection, TableMapping mapping, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = await ExecuteQueryAsync<T>(command, mapping);
                return result.First();
            }
        }

        internal static async Task<List<T>> ExecuteQueryAsync<T>(this DbCommand command, TableMapping map)
        {
            var result = new List<T>();
            using (var reader = await command.ExecuteReaderAsync())
            {
                var cols = new TableMapping.Column[reader.FieldCount];
                for (var i = 0; i < cols.Length; i++)
                {
                    var name = reader.GetName(i);
                    cols[i] = map.FindColumn(name);
                }

                while (await reader.ReadAsync())
                {
                    var obj = Activator.CreateInstance(map.MappedType);
                    for (var i = 0; i < cols.Length; i++)
                    {
                        var col = cols[i];
                        var val = reader.GetValue(i);
                        // TODO:
                        col.SetValue(obj, val);
                    }
                    result.Add((T)obj);
                }
            }
            return result;
        }

        internal static async Task<T> ExecuteQueryFirstOrDefaultAsync<T>(this DbCommand command, TableMapping map)
        {
            using (var reader = await command.ExecuteReaderAsync())
            {
                var cols = new TableMapping.Column[reader.FieldCount];
                for (var i = 0; i < cols.Length; i++)
                {
                    var name = reader.GetName(i);
                    cols[i] = map.FindColumn(name);
                }

                if (await reader.ReadAsync())
                {
                    var obj = Activator.CreateInstance(map.MappedType);
                    for (var i = 0; i < cols.Length; i++)
                    {
                        var col = cols[i];
                        var val = reader.GetValue(i);
                        // TODO:
                        col.SetValue(obj, val);
                    }
                    return (T)obj;
                }
                else
                {
                    return default(T);
                }
            }
        }

        public static async Task<IEnumerable<T>> QueryAsync<T>(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return await ExecuteQueryAsync<T>(command, GetMapping<T>());
            }
        }

        public static Task<IEnumerable<object>> QueryAsync(this DbConnection connection, Type type, string sql, object param = null) =>
            connection.QueryAsync(GetMapping(type), sql, param);

        private static async Task<IEnumerable<object>> QueryAsync(this DbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return await ExecuteQueryAsync<object>(command, mapping);
            }
        }

        public static Task<T> FindWithQueryAsync<T>(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQueryFirstOrDefaultAsync<T>(command, GetMapping<T>());
            }
        }

        public static Task<object> FindWithQueryAsync(this DbConnection connection, Type type, string sql, object param = null) =>
            connection.FindWithQueryAsync(GetMapping(type), sql, param);

        private static Task<object> FindWithQueryAsync(this DbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQueryFirstOrDefaultAsync<object>(command, mapping);
            }
        }

        public static Task<int> ExecuteAsync(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return command.ExecuteNonQueryAsync();
            }
        }

        public static async Task<T> ExecuteScalarAsync<T>(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var result = await command.ExecuteScalarAsync();
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

