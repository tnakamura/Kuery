using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Kuery
{
    public static partial class SqlHelper
    {
        public static TableQuery<T> Table<T>(this DbConnection connection)
        {
            return new TableQuery<T>(connection, new TableMapping(typeof(T)));
        }

        public static Task<int> InsertAsync<T>(this DbConnection connection, T item) =>
            connection.InsertAsync(item, typeof(T));

        public static Task<int> InsertAsync(this DbConnection connection, object item, Type type)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (type == null) throw new ArgumentNullException(nameof(type));

            using (var command = connection.CreateInsertCommand(item, type))
            {
                return command.ExecuteNonQueryAsync();
            }
        }


        public static int Insert<T>(this DbConnection connection, T item) =>
            connection.Insert(item, typeof(T));

        public static int Insert(this DbConnection connection, object item, Type type)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (type == null) throw new ArgumentNullException(nameof(type));

            using (var command = connection.CreateInsertCommand(item, type))
            {
                return command.ExecuteNonQuery();
            }
        }

        static DbCommand CreateInsertCommand(this DbConnection connection, object item, Type type)
        {
            var map = GetMapping(type);
            var columns = new StringBuilder();
            var values = new StringBuilder();
            var command = connection.CreateCommand();

            for (var i = 0; i < map.InsertColumns.Length; i++)
            {
                if (i > 0)
                {
                    columns.Append(",");
                    values.Append(",");
                }

                var col = map.InsertColumns[i];
                columns.Append("[" + col.Name + "]");

                var value = col.GetValue(item);
                if (value is null && col.IsNullable)
                {
                    values.Append("NULL");
                }
                else
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "@" + col.Name;
                    parameter.Value = col.GetValue(item);
                    command.Parameters.Add(parameter);
                    values.Append("@" + col.Name);
                }
            }

            command.CommandText = "insert into "
                + map.TableName
                + " ("
                + columns.ToString()
                + ") values ("
                + values.ToString()
                + ");";
            return command;
        }

        public static int InsertAll(this DbConnection connection, IEnumerable items, DbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                using (var command = connection.CreateInsertCommand(item, Orm.GetType(item)))
                {
                    if (transaction != null)
                    {
                        command.Transaction = transaction;
                    }
                    result += command.ExecuteNonQuery();
                }
            }
            return result;
        }

        public static int InsertAll(this DbConnection connection, IEnumerable items, Type type, DbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            if (type == null) throw new ArgumentNullException(nameof(type));

            var result = 0;
            foreach (var item in items)
            {
                using (var command = connection.CreateInsertCommand(item, type))
                {
                    if (transaction != null)
                    {
                        command.Transaction = transaction;
                    }
                    result += command.ExecuteNonQuery();
                }
            }
            return result;
        }

        public static async Task<int> InsertAllAsync(this DbConnection connection, IEnumerable items, DbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                using (var command = connection.CreateInsertCommand(item, Orm.GetType(item)))
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

        public static async Task<int> InsertAllAsync(this DbConnection connection, IEnumerable items, Type type, DbTransaction transaction = null)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                using (var command = connection.CreateInsertCommand(item, type))
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

        public static Task<int> UpdateAsync<T>(this DbConnection connection, T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            return connection.UpdateAsync(item, typeof(T));
        }

        public static Task<int> UpdateAsync(this DbConnection connection, object item, Type type)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (type == null) throw new ArgumentNullException(nameof(type));

            using (var command = connection.CreateUpdateCommand(item, type))
            {
                return command.ExecuteNonQueryAsync();
            }
        }

        public static int Update<T>(this DbConnection connection, T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            return connection.Update(item, typeof(T));
        }

        public static int Update(this DbConnection connection, object item, Type type)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (type == null) throw new ArgumentNullException(nameof(type));

            using (var command = connection.CreateUpdateCommand(item, type))
            {
                return command.ExecuteNonQuery();
            }
        }

        static DbCommand CreateUpdateCommand(this DbConnection connection, object item, Type type)
        {
            var mapping = GetMapping(type);
            if (mapping.PK == null)
            {
                throw new NotSupportedException(
                    $"Cannot update {mapping.TableName}: it has no PK");
            }

            var sql = new StringBuilder();
            sql.Append("update " + mapping.TableName);
            var command = connection.CreateCommand();

            var cols = mapping.Columns.Where(x => x != mapping.PK);
            var first = true;
            foreach (var col in cols)
            {
                var parameter = command.CreateParameter();
                parameter.ParameterName = "@" + col.Name;
                parameter.Value = col.GetValue(item);
                command.Parameters.Add(parameter);

                if (first)
                {
                    sql.Append(" set ");
                    first = false;
                }
                else
                {
                    sql.Append(",");
                }
                sql.Append(col.Name);
                sql.Append(" = ");
                sql.Append(parameter.ParameterName);
            }

            sql.Append(" where ");
            sql.Append(mapping.PK.Name);
            sql.Append(" = @");
            sql.Append(mapping.PK.Name);
            var pkParamter = command.CreateParameter();
            pkParamter.ParameterName = "@" + mapping.PK.Name;
            pkParamter.Value = mapping.PK.GetValue(item);
            command.Parameters.Add(pkParamter);

            command.CommandText = sql.ToString();
            return command;
        }

        public static int UpdateAll(this DbConnection connection, IEnumerable items, DbTransaction transaction = null)
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
                    result += command.ExecuteNonQuery();
                }
            }
            return result;
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

        public static int InsertOrReplace(this DbConnection connection, object item)
        {
            if (item == null)
            {
                return 0;
            }
            return connection.InsertOrReplace(item, Orm.GetType(item));
        }

        public static int InsertOrReplace(this DbConnection connection, object item, Type type)
        {
            if (item == null)
            {
                return 0;
            }
            using (var command = connection.CreateMergeCommand(item, type))
            {
                return command.ExecuteNonQuery();
            }
        }

        static DbCommand CreateMergeCommand(this DbConnection connection, object item, Type type)
        {
            var map = GetMapping(type);
            var command = connection.CreateCommand();

            var sb = new StringBuilder();
            sb.AppendLine("merge into");
            sb.AppendLine("  [" + map.TableName + "] as a");
            sb.AppendLine("using");
            sb.AppendLine("  (");
            sb.AppendLine("    select");

            for (var i = 0; i < map.InsertOrReplaceColumns.Length; i++)
            {
                var c = map.InsertOrReplaceColumns[i];
                sb.Append("      @" + c.Name + " as [" + c.Name + "]");
                if (i < map.InsertOrReplaceColumns.Length - 1)
                {
                    sb.Append(",");
                }
                sb.AppendLine();
            }

            sb.AppendLine("  ) as b");
            sb.AppendLine("on");
            sb.AppendLine("  (");
            sb.AppendLine("    a.[" + map.PK.Name + "] = b.[" + map.PK.Name + "]");
            sb.AppendLine("  )");
            sb.AppendLine("when matched then");
            sb.AppendLine("  update set");

            for (var i = 0; i < map.InsertOrReplaceColumns.Length; i++)
            {
                var c = map.InsertOrReplaceColumns[i];
                if (c.IsPK)
                {
                    continue;
                }
                sb.Append("    [" + c.Name + "] = b.[" + c.Name + "]");
                if (i < map.InsertOrReplaceColumns.Length - 1)
                {
                    sb.Append(",");
                }
                sb.AppendLine();
            }

            sb.AppendLine("when not matched then");
            sb.AppendLine("  insert");
            sb.AppendLine("  (");

            for (var i = 0; i < map.InsertOrReplaceColumns.Length; i++)
            {
                var c = map.InsertOrReplaceColumns[i];
                sb.Append("    [" + c.Name + "]");
                if (i < map.InsertOrReplaceColumns.Length - 1)
                {
                    sb.Append(",");
                }
                sb.AppendLine();
            }

            sb.AppendLine("  )");
            sb.AppendLine("  values");
            sb.AppendLine("  (");

            for (var i = 0; i < map.InsertOrReplaceColumns.Length; i++)
            {
                var c = map.InsertOrReplaceColumns[i];
                sb.Append("    b.[" + c.Name + "]");
                if (i < map.InsertOrReplaceColumns.Length - 1)
                {
                    sb.Append(",");
                }
                sb.AppendLine();
            }

            sb.AppendLine("  )");
            sb.AppendLine(";");

            command.CommandText = sb.ToString();

            for (var i = 0; i < map.InsertOrReplaceColumns.Length; i++)
            {
                var c = map.InsertOrReplaceColumns[i];
                var p = command.CreateParameter();
                p.ParameterName = "@" + c.Name;
                p.Value = c.GetValue(item);
                command.Parameters.Add(p);
            }

            return command;
        }

        public static Task<int> InsertOrReplaceAsync(this DbConnection connection, object item)
        {
            if (item == null)
            {
                return Task.FromResult(0);
            }
            return connection.InsertOrReplaceAsync(item, Orm.GetType(item));
        }

        public static Task<int> InsertOrReplaceAsync(this DbConnection connection, object item, Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }
            if (item == null)
            {
                return Task.FromResult(0);
            }
            using (var command = connection.CreateMergeCommand(item, type))
            {
                return command.ExecuteNonQueryAsync();
            }
        }

        public static Task<int> DeleteAsync<T>(this DbConnection connection, T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            using (var command = connection.CreateDeleteCommand(item, typeof(T)))
            {
                return command.ExecuteNonQueryAsync();
            }
        }

        static DbCommand CreateDeleteCommand(this DbConnection connection, object item, Type type)
        {
            var map = GetMapping(type);
            if (map.PK == null)
            {
                throw new NotSupportedException(
                    $"Cannot update {map.TableName}: it has no PK");
            }

            return connection.CreateDeleteCommand(map.PK.GetValue(item), map);
        }

        static DbCommand CreateDeleteCommand(this DbConnection connection, object primaryKey, TableMapping map)
        {
            var pk = map.PK;
            if (pk == null)
            {
                throw new NotSupportedException(
                    $"Cannot delete {map.TableName}: it has no PK");
            }

            var query = $"DELETE FROM [{map.TableName}] where [{pk.Name}] = @pk";

            var command = connection.CreateCommand();
            command.CommandText = query;

            var pkParameter = command.CreateParameter();
            pkParameter.ParameterName = "@pk";
            pkParameter.Value = primaryKey;
            command.Parameters.Add(pkParameter);

            return command;
        }

        public static int Delete<T>(this DbConnection connection, T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            using (var command = connection.CreateDeleteCommand(item, typeof(T)))
            {
                return command.ExecuteNonQuery();
            }
        }

        public static int Delete<T>(this DbConnection connection, object primaryKey)
        {
            return Delete(connection, primaryKey, GetMapping<T>());
        }

        public static int Delete(this DbConnection connection, object primaryKey, TableMapping map)
        {
            if (map == null) throw new ArgumentNullException(nameof(map));

            using (var command = connection.CreateDeleteCommand(primaryKey, map))
            {
                return command.ExecuteNonQuery();
            }
        }

        public static Task<int> DeleteAsync<T>(this DbConnection connection, object primaryKey)
        {
            return DeleteAsync(connection, primaryKey, GetMapping<T>());
        }

        public static Task<int> DeleteAsync(this DbConnection connection, object primaryKey, TableMapping map)
        {
            if (map == null) throw new ArgumentNullException(nameof(map));

            using (var command = connection.CreateDeleteCommand(primaryKey, map))
            {
                return command.ExecuteNonQueryAsync();
            }
        }

        public static T Find<T>(this DbConnection connection, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));

            var map = GetMapping(typeof(T));
            return connection.Find<T>(pk, map);
        }

        public static Task<T> FindAsync<T>(this DbConnection connection, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));

            var map = GetMapping(typeof(T));
            return connection.FindAsync<T>(pk, map);
        }

        public static T Find<T>(this DbConnection connection, object pk, TableMapping mapping)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = ExecuteQuery<T>(command, mapping);
                return result.FirstOrDefault();
            }
        }

        public static async Task<T> FindAsync<T>(this DbConnection connection, object pk, TableMapping mapping)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = await ExecuteQueryAsync<T>(command, mapping);
                return result.FirstOrDefault();
            }
        }

        public static T Find<T>(this DbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().FirstOrDefault(predicate);
        }

        public static Task<T> FindAsync<T>(this DbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().FirstOrDefaultAsync(predicate);
        }

        public static T Get<T>(this DbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().First(predicate);
        }

        public static Task<T> GetAsync<T>(this DbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().FirstAsync(predicate);
        }

        public static T Get<T>(this DbConnection connection, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));

            var map = GetMapping(typeof(T));
            return connection.Get<T>(pk, map);
        }

        public static Task<T> GetAsync<T>(this DbConnection connection, object pk)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));

            var map = GetMapping(typeof(T));
            return connection.GetAsync<T>(pk, map);
        }

        public static T Get<T>(this DbConnection connection, object pk, TableMapping mapping)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = ExecuteQuery<T>(command, mapping);
                return result.First();
            }
        }

        public static async Task<T> GetAsync<T>(this DbConnection connection, object pk, TableMapping mapping)
        {
            if (pk == null) throw new ArgumentNullException(nameof(pk));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = await ExecuteQueryAsync<T>(command, mapping);
                return result.First();
            }
        }

        static DbCommand CreateGetByPrimaryKeyCommand(this DbConnection connection, TableMapping map, object pk)
        {
            var command = connection.CreateCommand();
            command.CommandText = map.GetByPrimaryKeySql;
            if (map.PK != null)
            {
                var pkParameter = command.CreateParameter();
                pkParameter.ParameterName = "@" + map.PK.Name;
                pkParameter.Value = pk;
                command.Parameters.Add(pkParameter);
            }
            return command;
        }

        internal static List<T> ExecuteQuery<T>(this DbCommand command, TableMapping map)
        {
            var result = new List<T>();
            using (var reader = command.ExecuteReader())
            {
                var cols = new TableMapping.Column[reader.FieldCount];
                for (var i = 0; i < cols.Length; i++)
                {
                    var name = reader.GetName(i);
                    cols[i] = map.FindColumn(name);
                }

                while (reader.Read())
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

        internal static T ExecuteQueryFirstOrDefault<T>(this DbCommand command, TableMapping map)
        {
            using (var reader = command.ExecuteReader())
            {
                var cols = new TableMapping.Column[reader.FieldCount];
                for (var i = 0; i < cols.Length; i++)
                {
                    var name = reader.GetName(i);
                    cols[i] = map.FindColumn(name);
                }

                if (reader.Read())
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

        static readonly Dictionary<string, TableMapping> _mappings = new Dictionary<string, TableMapping>();

        public static TableMapping GetMapping<T>(CreateFlags createFlags = CreateFlags.None)
        {
            return GetMapping(typeof(T), createFlags);
        }

        public static TableMapping GetMapping(Type type, CreateFlags createFlags = CreateFlags.None)
        {
            TableMapping map;
            var key = type.FullName;
            lock (_mappings)
            {
                if (_mappings.TryGetValue(key, out map))
                {
                    if (createFlags != CreateFlags.None && createFlags != map.CreateFlags)
                    {
                        map = new TableMapping(type, createFlags);
                        _mappings[key] = map;
                    }
                }
                else
                {
                    map = new TableMapping(type, createFlags);
                    _mappings.Add(key, map);
                }
            }
            return new TableMapping(type);
        }

        public static IEnumerable<T> Query<T>(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQuery<T>(command, GetMapping<T>());
            }
        }

        public static async Task<IEnumerable<T>> QueryAsync<T>(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return await ExecuteQueryAsync<T>(command, GetMapping<T>());
            }
        }

        public static IEnumerable<object> Query(this DbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQuery<object>(command, mapping);
            }
        }

        public static async Task<IEnumerable<object>> QueryAsync(this DbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));

            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return await ExecuteQueryAsync<object>(command, mapping);
            }
        }

        public static T FindWithQuery<T>(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQueryFirstOrDefault<T>(command, GetMapping<T>());
            }
        }

        public static Task<T> FindWithQueryAsync<T>(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQueryFirstOrDefaultAsync<T>(command, GetMapping<T>());
            }
        }

        public static object FindWithQuery(this DbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQueryFirstOrDefault<object>(command, mapping);
            }
        }

        public static Task<object> FindWithQueryAsync(this DbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQueryFirstOrDefaultAsync<object>(command, mapping);
            }
        }

        static DbCommand CreateParameterizedCommand(this DbConnection connection, string sql, object param = null)
        {
            var command = connection.CreateCommand();
            command.CommandText = sql;

            if (param != null)
            {
                if (param is IDictionary<string, object> dict)
                {
                    foreach (var kvp in dict)
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = "@" + kvp.Key;
                        parameter.Value = kvp.Value;
                        command.Parameters.Add(parameter);
                    }
                }
                else
                {
                    var paramType = param.GetType();
                    var properties = paramType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                    foreach (var property in properties)
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = "@" + property.Name;
                        parameter.Value = property.GetValue(param);
                        command.Parameters.Add(parameter);
                    }
                    var fields = paramType.GetFields(BindingFlags.Public | BindingFlags.Instance);
                    foreach (var field in fields)
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = "@" + field.Name;
                        parameter.Value = field.GetValue(param);
                        command.Parameters.Add(parameter);
                    }
                }
            }

            return command;
        }

        public static int Execute(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return command.ExecuteNonQuery();
            }
        }

        public static Task<int> ExecuteAsync(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return command.ExecuteNonQueryAsync();
            }
        }

        public static T ExecuteScalar<T>(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var result = command.ExecuteScalar();
                if (result is DBNull)
                {
                    return default;
                }
                else
                {
                    return (T)result;
                }
            }
        }

        public static async Task<T> ExecuteScalarAsync<T>(this DbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var result = await command.ExecuteScalarAsync();
                if (result is DBNull)
                {
                    return default;
                }
                else
                {
                    return (T)result;
                }
            }
        }
    }

    [AttributeUsage(AttributeTargets.Class)]
    public sealed class TableAttribute : Attribute
    {
        public string Name { get; set; }

        public bool WithoutRowId { get; set; }

        public TableAttribute(string name)
        {
            Name = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class ColumnAttribute : Attribute
    {
        public string Name { get; set; }

        public ColumnAttribute(string name)
        {
            Name = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class PrimaryKeyAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class AutoIncrementAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class IndexedAttribute : Attribute
    {
        public string Name { get; set; }

        public int Order { get; set; }

        public virtual bool Unique { get; set; }

        public IndexedAttribute()
        {
        }

        public IndexedAttribute(string name, int order)
        {
            Name = name;
            Order = order;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class IgnoreAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class UniqueAttribute : IndexedAttribute
    {
        public override bool Unique
        {
            get => true;
            set { }
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MaxLengthAttribute : Attribute
    {
        public int Value { get; private set; }

        public MaxLengthAttribute(int length)
        {
            Value = length;
        }
    }

    public sealed class PreserveAttribute : Attribute
    {
        public bool AllMembers { get; set; }

        public bool Conditional { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class CollationAttribute : Attribute
    {
        public string Value { get; private set; }

        public CollationAttribute(string collation)
        {
            Value = collation;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class NotNullAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Enum)]
    public sealed class StoreAsTextAttribute : Attribute
    {
    }

    public sealed class TableMapping
    {
        public Type MappedType { get; private set; }

        public string TableName { get; private set; }

        public bool WithoutRowId { get; private set; }

        public Column[] Columns { get; private set; }

        public Column PK { get; private set; }

        public string GetByPrimaryKeySql { get; private set; }

        public CreateFlags CreateFlags { get; private set; }

        readonly Column _autoPk;

        public TableMapping(Type type, CreateFlags createFlags = CreateFlags.None)
        {
            MappedType = type;
            CreateFlags = createFlags;

            var typeInfo = type.GetTypeInfo();
            var tableAttr = typeInfo.CustomAttributes
                .Where(x => x.AttributeType == typeof(TableAttribute))
                .Select(x => (TableAttribute)Orm.InflateAttribute(x))
                .FirstOrDefault();

            TableName = (tableAttr != null && !string.IsNullOrEmpty(tableAttr.Name)) ?
                tableAttr.Name :
                MappedType.Name;
            WithoutRowId = tableAttr != null ? tableAttr.WithoutRowId : false;

            var props = new List<PropertyInfo>();
            var baseType = type;
            var propNames = new HashSet<string>();
            while (baseType != typeof(object))
            {
                var ti = baseType.GetTypeInfo();
                var newProps = (
                    from p in ti.DeclaredProperties
                    where !propNames.Contains(p.Name)
                    where p.CanRead
                    where p.CanWrite
                    where p.GetMethod != null
                    where p.SetMethod != null
                    where p.GetMethod.IsPublic
                    where p.SetMethod.IsPublic
                    where !p.GetMethod.IsStatic
                    where !p.SetMethod.IsStatic
                    select p
                ).ToList();
                foreach (var p in newProps)
                {
                    propNames.Add(p.Name);
                }
                props.AddRange(newProps);
                baseType = ti.BaseType;
            }

            var cols = new List<Column>();
            foreach (var p in props)
            {
                var ignore = p.IsDefined(typeof(IgnoreAttribute), true);
                if (!ignore)
                {
                    cols.Add(new Column(p, createFlags));
                }
            }
            Columns = cols.ToArray();
            foreach (var c in Columns)
            {
                if (c.IsAutoInc && c.IsPK)
                {
                    _autoPk = c;
                }
                if (c.IsPK)
                {
                    PK = c;
                }
            }

            HasAutoIncPK = _autoPk != null;

            if (PK != null)
            {
                GetByPrimaryKeySql = $"select * from [{TableName}] where [{PK.Name}] = @{PK.Name}";
            }
            else
            {
                GetByPrimaryKeySql = $"select top 1 * from [{TableName}]";
            }

            InsertColumns = Columns.Where(c => !c.IsAutoInc).ToArray();
            InsertOrReplaceColumns = Columns.ToArray();
        }

        public bool HasAutoIncPK { get; private set; }

        public void SetAutoIncPk(object obj, long id)
        {
            if (_autoPk != null)
            {
                _autoPk.SetValue(obj, Convert.ChangeType(id, _autoPk.ColumnType, null));
            }
        }

        public Column[] InsertColumns { get; }

        public Column[] InsertOrReplaceColumns { get; }

        public Column FindColumnWithPropertyName(string propertyName) =>
            Columns.FirstOrDefault(x => x.PropertyName == propertyName);

        public Column FindColumn(string columnName) =>
            Columns.FirstOrDefault(x => x.Name.ToLower() == columnName.ToLower());

        public sealed class Column
        {
            readonly PropertyInfo _prop;

            public string Name { get; private set; }

            public PropertyInfo PropertyInfo => _prop;

            public string PropertyName => _prop.Name;

            public Type ColumnType { get; private set; }

            public string Collation { get; private set; }

            public bool IsAutoInc { get; private set; }

            public bool IsAutoGuid { get; private set; }

            public bool IsPK { get; private set; }

            public IEnumerable<IndexedAttribute> Indices { get; set; }

            public bool IsNullable { get; private set; }

            public int? MaxStringLength { get; private set; }

            public bool StoreAsText { get; private set; }

            public Column(PropertyInfo prop, CreateFlags createFlags = CreateFlags.None)
            {
                var colAttr = prop.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(ColumnAttribute));

                _prop = prop;
                Name = (colAttr != null && colAttr.ConstructorArguments.Count > 0) ?
                    colAttr.ConstructorArguments[0].Value?.ToString() :
                    prop.Name;

                ColumnType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                Collation = Orm.Collation(prop);

                IsPK = Orm.IsPK(prop) ||
                    (((createFlags & CreateFlags.ImplicitPK) == CreateFlags.ImplicitPK) &&
                    string.Compare(prop.Name, Orm.ImplicitPkName, StringComparison.OrdinalIgnoreCase) == 0);

                var isAuto = Orm.IsAutoInc(prop) ||
                    (IsPK && ((createFlags & CreateFlags.AutoIncPK) == CreateFlags.AutoIncPK));
                IsAutoGuid = isAuto && ColumnType == typeof(Guid);
                IsAutoInc = isAuto && !IsAutoGuid;

                Indices = Orm.GetIndices(prop);
                if (!Indices.Any() &&
                    !IsPK &&
                    (createFlags & CreateFlags.ImplicitIndex) == CreateFlags.ImplicitIndex &&
                    Name.EndsWith(Orm.ImplicitIndexSuffix, StringComparison.OrdinalIgnoreCase))
                {
                    Indices = new IndexedAttribute[]
                    {
                        new IndexedAttribute()
                    };
                }
                IsNullable = !(IsPK || Orm.IsMarkedNotNull(prop));
                MaxStringLength = Orm.MaxStringLength(prop);

                StoreAsText = prop.PropertyType
                    .GetTypeInfo()
                    .CustomAttributes
                    .Any(x => x.AttributeType == typeof(StoreAsTextAttribute));
            }

            public void SetValue(object obj, object val)
            {
                if (val != null && ColumnType.GetTypeInfo().IsEnum)
                {
                    _prop.SetValue(obj, Enum.ToObject(ColumnType, val));
                }
                else
                {
                    _prop.SetValue(obj, val, null);
                }
            }

            public object GetValue(object obj)
            {
                return _prop.GetValue(obj, null);
            }
        }
    }

    class EnumCacheInfo
    {
        public EnumCacheInfo(Type type)
        {
            var typeInfo = type.GetTypeInfo();

            IsEnum = typeInfo.IsEnum;

            if (IsEnum)
            {
                StoreAsText = typeInfo.CustomAttributes
                    .Any(x => x.AttributeType == typeof(StoreAsTextAttribute));
                if (StoreAsText)
                {
                    EnumValues = new Dictionary<int, string>();
                    foreach (var e in Enum.GetValues(type))
                    {
                        EnumValues[Convert.ToInt32(e)] = e.ToString();
                    }
                }
            }
        }

        public bool IsEnum { get; private set; }

        public bool StoreAsText { get; private set; }

        public Dictionary<int, string> EnumValues { get; private set; }
    }

    static class EnumCache
    {
        static readonly Dictionary<Type, EnumCacheInfo> Cache = new Dictionary<Type, EnumCacheInfo>();

        public static EnumCacheInfo GetInfo<T>()
        {
            return GetInfo(typeof(T));
        }

        public static EnumCacheInfo GetInfo(Type type)
        {
            lock (Cache)
            {
                if (!Cache.TryGetValue(type, out var info))
                {
                    info = new EnumCacheInfo(type);
                    Cache[type] = info;
                }
                return info;
            }
        }
    }

    public static class Orm
    {
        public const int DefaultMaxStringLength = 140;
        public const string ImplicitPkName = "Id";
        public const string ImplicitIndexSuffix = "Id";

        public static Type GetType(object obj)
        {
            if (obj == null)
            {
                return typeof(object);
            }
            var rt = obj as IReflectableType;
            if (rt != null)
            {
                return rt.GetTypeInfo().AsType();
            }
            return obj.GetType();
        }

        public static string SqlDecl(TableMapping.Column p, bool storeDateTimeAsTicks, bool storeTimeSpanAsTicks)
        {
            var decl = "\"" +
                p.Name +
                "\" " +
                SqlType(p, storeDateTimeAsTicks, storeTimeSpanAsTicks)
                + " ";
            if (p.IsPK)
            {
                decl += "primary key ";
            }
            if (p.IsAutoInc)
            {
                decl += "autoincrement ";
            }
            if (!p.IsNullable)
            {
                decl += "not null ";
            }
            if (!string.IsNullOrEmpty(p.Collation))
            {
                decl += "collate " + p.Collation + " ";
            }
            return decl;
        }

        public static string SqlType(TableMapping.Column p, bool storeDateTimeAsTicks, bool storeTimeSpanAsTicks)
        {
            var clrType = p.ColumnType;
            if (clrType == typeof(bool) ||
                clrType == typeof(byte) ||
                clrType == typeof(ushort) ||
                clrType == typeof(sbyte) ||
                clrType == typeof(short) ||
                clrType == typeof(int) ||
                clrType == typeof(uint) ||
                clrType == typeof(long))
            {
                return "integer";
            }
            else if (clrType == typeof(float) ||
                clrType == typeof(double) ||
                clrType == typeof(decimal))
            {
                return "float";
            }
            else if (clrType == typeof(string) ||
                clrType == typeof(StringBuilder) ||
                clrType == typeof(Uri) ||
                clrType == typeof(UriBuilder))
            {
                var len = p.MaxStringLength;
                if (len.HasValue)
                {
                    return "varchar(" + len.Value + ")";
                }
                else
                {
                    return "varchar";
                }
            }
            else if (clrType == typeof(TimeSpan))
            {
                return storeTimeSpanAsTicks ? "bigint" : "time";
            }
            else if (clrType == typeof(DateTime))
            {
                return storeDateTimeAsTicks ? "bigint" : "datetime";
            }
            else if (clrType == typeof(DateTimeOffset))
            {
                return "bigint";
            }
            else if (clrType.GetTypeInfo().IsEnum)
            {
                if (p.StoreAsText)
                {
                    return "varchar";
                }
                else
                {
                    return "integer";
                }
            }
            else if (clrType == typeof(byte[]))
            {
                return "blob";
            }
            else if (clrType == typeof(Guid))
            {
                return "varchar(36)";
            }
            else
            {
                throw new NotSupportedException(
                    $"Don't know about {clrType}");
            }
        }

        public static bool IsPK(MemberInfo p)
        {
            return p.CustomAttributes.Any(x => x.AttributeType == typeof(PrimaryKeyAttribute));
        }

        public static string Collation(MemberInfo p)
        {
            return p.CustomAttributes
                .Where(x => x.AttributeType == typeof(CollationAttribute))
                .Select(x =>
                {
                    var args = x.ConstructorArguments;
                    return args.Count > 0 ? ((args[0].Value as string) ?? "") : "";
                })
                .FirstOrDefault() ?? "";
        }

        public static bool IsAutoInc(MemberInfo p)
        {
            return p.CustomAttributes.Any(x => x.AttributeType == typeof(AutoIncrementAttribute));
        }

        public static FieldInfo GetField(TypeInfo t, string name)
        {
            var f = t.GetDeclaredField(name);
            if (f != null)
            {
                return f;
            }
            return GetField(t.BaseType.GetTypeInfo(), name);
        }

        public static PropertyInfo GetProperty(TypeInfo t, string name)
        {
            var f = t.GetDeclaredProperty(name);
            if (f != null)
            {
                return f;
            }
            return GetProperty(t.BaseType.GetTypeInfo(), name);
        }

        public static object InflateAttribute(CustomAttributeData x)
        {
            var atype = x.AttributeType;
            var typeInfo = atype.GetTypeInfo();
            var args = x.ConstructorArguments.Select(a => a.Value).ToArray();
            var r = Activator.CreateInstance(x.AttributeType, args);
            foreach (var arg in x.NamedArguments)
            {
                if (arg.IsField)
                {
                    GetField(typeInfo, arg.MemberName).SetValue(r, arg.TypedValue.Value);
                }
                else
                {
                    GetProperty(typeInfo, arg.MemberName).SetValue(r, arg.TypedValue.Value);
                }
            }
            return r;
        }

        public static IEnumerable<IndexedAttribute> GetIndices(MemberInfo p)
        {
            var indexedInfo = typeof(IndexedAttribute).GetTypeInfo();
            return p.CustomAttributes
                .Where(x => indexedInfo.IsAssignableFrom(x.AttributeType.GetTypeInfo()))
                .Select(x => (IndexedAttribute)InflateAttribute(x));
        }

        public static int? MaxStringLength(PropertyInfo p)
        {
            var attr = p.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(MaxLengthAttribute));
            if (attr != null)
            {
                var attrv = (MaxLengthAttribute)InflateAttribute(attr);
                return attrv.Value;
            }
            return null;
        }

        public static bool IsMarkedNotNull(MemberInfo p)
        {
            return p.CustomAttributes.Any(x => x.AttributeType == typeof(NotNullAttribute));
        }
    }

    [Flags]
    public enum CreateFlags
    {
        None = 0x000,
        ImplicitPK = 0x001,
        ImplicitIndex = 0x002,
        AllImplicit = 0x003,
        AutoIncPK = 0x004,
        FullTextSearch3 = 0x100,
        FullTextSearch4 = 0x200,
    }

    public abstract class BaseTableQuery
    {
        protected class Ordering
        {
            public string ColumnName { get; set; }

            public bool Ascending { get; set; }
        }
    }

    public class TableQuery<T> : BaseTableQuery, IEnumerable<T>
    {
        public DbConnection Connection { get; private set; }

        public TableMapping Table { get; private set; }

        Expression _where;

        List<Ordering> _orderBys;

        int? _limit;

        int? _offset;

        BaseTableQuery _joinInner;

        Expression _joinInnerKeySelector;

        BaseTableQuery _joinOuter;

        Expression _joinOuterKeySelector;

        Expression _joinSelector;

        Expression _selector;

        bool _deferred;


        public TableQuery(DbConnection connection, TableMapping table)
        {
            Connection = connection;
            Table = table;
        }

        public TableQuery(DbConnection connection)
        {
            Connection = connection;
            Table = SqlHelper.GetMapping(typeof(T));
        }

        TableQuery<U> Clone<U>()
        {
            var q = new TableQuery<U>(Connection, Table);
            q._where = _where;
            q._deferred = _deferred;
            if (_orderBys != null)
            {
                q._orderBys = new List<Ordering>(_orderBys);
            }
            q._limit = _limit;
            q._offset = _offset;
            q._joinInner = _joinInner;
            q._joinInnerKeySelector = _joinInnerKeySelector;
            q._joinOuter = _joinOuter;
            q._joinOuterKeySelector = _joinOuterKeySelector;
            q._joinSelector = _joinSelector;
            q._selector = _selector;
            return q;
        }

        public TableQuery<T> Where(Expression<Func<T, bool>> predExpr)
        {
            if (predExpr.NodeType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)predExpr;
                var pred = lambda.Body;
                var q = Clone<T>();
                q.AddWhere(pred);
                return q;
            }
            else
            {
                throw new NotSupportedException("Must be a predicate");
            }
        }

        public int Delete()
        {
            return Delete(null);
        }

        public int Delete(Expression<Func<T, bool>> predExpr)
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
            var cmdText = "delete from \"" + Table.TableName + "\"";
            var w = CompileExpr(pred, args);
            cmdText += " where " + w.CommandText;

            using (var command = Connection.CreateCommand())
            {
                command.CommandText = cmdText;
                for (var i = 0; i < args.Count; i++)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "@p" + (i + 1).ToString();
                    parameter.Value = args[i];
                    command.Parameters.Add(parameter);
                }
                var result = command.ExecuteNonQuery();
                return result;
            }
        }

        public Task<int> DeleteAsync() => DeleteAsync(null);

        public Task<int> DeleteAsync(Expression<Func<T, bool>> predExpr)
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
            var cmdText = "delete from \"" + Table.TableName + "\"";
            var w = CompileExpr(pred, args);
            cmdText += " where " + w.CommandText;

            using (var command = Connection.CreateCommand())
            {
                command.CommandText = cmdText;
                for (var i = 0; i < args.Count; i++)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "@p" + (i + 1).ToString();
                    parameter.Value = args[i];
                    command.Parameters.Add(parameter);
                }
                var result = command.ExecuteNonQueryAsync();
                return result;
            }
        }

        public TableQuery<T> Take(int n)
        {
            var q = Clone<T>();
            q._limit = n;
            return q;
        }

        public TableQuery<T> Skip(int n)
        {
            var q = Clone<T>();
            q._offset = n;
            return q;
        }

        public T ElementAt(int index)
        {
            return Skip(index).Take(1).First();
        }

        public Task<T> ElementAtAsync(int index)
        {
            return Skip(index).Take(1).FirstAsync();
        }

        public TableQuery<T> Deferred()
        {
            var q = Clone<T>();
            q._deferred = true;
            return q;
        }

        public TableQuery<T> OrderBy<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, true);
        }

        public TableQuery<T> OrderByDescending<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, false);
        }

        public TableQuery<T> ThenBy<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, true);
        }

        public TableQuery<T> ThenByDescending<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, false);
        }

        TableQuery<T> AddOrderBy<U>(Expression<Func<T, U>> orderExpr, bool asc)
        {
            if (orderExpr.NodeType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)orderExpr;

                MemberExpression mem = null;

                var unary = lambda.Body as UnaryExpression;
                if (unary != null && unary.NodeType == ExpressionType.Convert)
                {
                    mem = unary.Operand as MemberExpression;
                }
                else
                {
                    mem = lambda.Body as MemberExpression;
                }

                if (mem != null && mem.Expression.NodeType == ExpressionType.Parameter)
                {
                    var q = Clone<T>();
                    if (q._orderBys == null)
                    {
                        q._orderBys = new List<Ordering>();
                    }
                    q._orderBys.Add(new Ordering
                    {
                        ColumnName = Table.FindColumnWithPropertyName(mem.Member.Name).Name,
                        Ascending = asc,
                    });
                    return q;
                }
                else
                {
                    throw new NotSupportedException(
                        "Order By does not support: " + orderExpr);
                }
            }
            else
            {
                throw new NotSupportedException("Must be a predicate");
            }
        }

        public List<T> ToList()
        {
            return GenerateCommand("*").ExecuteQuery<T>(Table);
        }

        public T[] ToArray()
        {
            return GenerateCommand("*").ExecuteQuery<T>(Table).ToArray();
        }

        public Task<List<T>> ToListAsync()
        {
            return GenerateCommand("*").ExecuteQueryAsync<T>(Table);
        }

        public async Task<T[]> ToArrayAsync()
        {
            return (await ToListAsync()).ToArray();
        }

        void AddWhere(Expression pred)
        {
            if (_where == null)
            {
                _where = pred;
            }
            else
            {
                _where = Expression.AndAlso(_where, pred);
            }
        }

        DbCommand GenerateCommand(string selectionList)
        {
            if (_joinInner != null && _joinOuter != null)
            {
                throw new NotSupportedException("Joins are not supported.");
            }
            var cmdText = new StringBuilder();
            cmdText.Append("select ");
            if (_limit.HasValue && !(_offset.HasValue && _offset.Value > 0))
            {
                cmdText.Append(" top ");
                cmdText.Append(_limit.Value);
                cmdText.Append(" ");
            }
            cmdText.Append(selectionList);
            cmdText.Append(" from [");
            cmdText.Append(Table.TableName);
            cmdText.Append("]");
            var args = new List<object>();
            if (_where != null)
            {
                var w = CompileExpr(_where, args);
                cmdText.Append(" where ");
                cmdText.Append(w.CommandText);
            }
            if (_orderBys != null && _orderBys.Count > 0)
            {
                cmdText.Append(" order by ");
                for (var i = 0; i < _orderBys.Count; i++)
                {
                    if (i > 0)
                    {
                        cmdText.Append(" , ");
                    }
                    var o = _orderBys[i];
                    cmdText.Append("[");
                    cmdText.Append(o.ColumnName);
                    cmdText.Append("]");
                    if (!o.Ascending)
                    {
                        cmdText.Append(" desc ");
                    }
                }
            }
            if (_offset.HasValue && _offset.Value > 0)
            {
                if (_orderBys == null || _orderBys.Count == 0)
                {
                    cmdText.Append(" order by [");
                    cmdText.Append(Table.PK.Name);
                    cmdText.Append("]");
                }

                cmdText.Append(" offset ");
                cmdText.Append(_offset.Value);
                cmdText.Append(" rows");

                if (_limit.HasValue)
                {
                    cmdText.Append(" fetch next ");
                    cmdText.Append(_limit.Value);
                    cmdText.Append(" rows only");
                }
            }
            var cmd = Connection.CreateCommand();
            cmd.CommandText = cmdText.ToString();
            for (var i = 0; i < args.Count; i++)
            {
                var p = cmd.CreateParameter();
                p.Value = args[i];
                p.ParameterName = "@p" + (i + 1).ToString();
                cmd.Parameters.Add(p);
            }
            return cmd;
        }

        class CompileResult
        {
            public string CommandText { get; set; }

            public object Value { get; set; }
        }

        CompileResult CompileExpr(Expression expr, List<object> queryArgs)
        {
            if (expr == null)
            {
                throw new NotSupportedException("Expression is NLL");
            }
            else if (expr is BinaryExpression)
            {
                var bin = (BinaryExpression)expr;
                var leftr = CompileExpr(bin.Left, queryArgs);
                var rightr = CompileExpr(bin.Right, queryArgs);

                string text;
                if (leftr.CommandText == ("@p" + queryArgs.Count.ToString()) && leftr.Value == null)
                {
                    text = CompileNullBinaryExpression(bin, rightr);
                }
                else if (rightr.CommandText == ("@p" + queryArgs.Count.ToString()) && rightr.Value == null)
                {
                    text = CompileNullBinaryExpression(bin, leftr);
                }
                else
                {
                    text = "(" + leftr.CommandText + " " + GetSqlName(bin) + " " + rightr.CommandText + ")";
                }
                return new CompileResult { CommandText = text };
            }
            else if (expr.NodeType == ExpressionType.Not)
            {
                var operandExpr = ((UnaryExpression)expr).Operand;
                var opr = CompileExpr(operandExpr, queryArgs);
                var val = opr.Value;
                if (val is bool)
                {
                    val = !((bool)val);
                }
                return new CompileResult
                {
                    CommandText = "not(" + opr.CommandText + ")",
                    Value = val,
                };
            }
            else if (expr.NodeType == ExpressionType.Call)
            {
                var call = (MethodCallExpression)expr;
                var args = new CompileResult[call.Arguments.Count];
                var obj = call.Object != null ? CompileExpr(call.Object, queryArgs) : null;

                for (var i = 0; i < args.Length; i++)
                {
                    args[i] = CompileExpr(call.Arguments[i], queryArgs);
                }

                var sqlCall = "";
                if (call.Method.Name == "Like" && args.Length == 2)
                {
                    sqlCall = "(" + args[0].CommandText + " like " + args[1].CommandText + ")";
                }
                else if (call.Method.Name == "Contains" && args.Length == 2)
                {
                    sqlCall = "(" + args[1].CommandText + " in " + args[0].CommandText + ")";
                }
                else if (call.Method.Name == "Contains" && args.Length == 1)
                {
                    if (call.Object != null && call.Object.Type == typeof(string))
                    {
                        sqlCall = "( instr(" + obj.CommandText + "," + args[0].CommandText + ") > 0)";
                    }
                    else
                    {
                        sqlCall = "(" + args[0].CommandText + " in " + obj.CommandText + ")";
                    }
                }
                else if (call.Method.Name == "StartsWith" && args.Length >= 1)
                {
                    var startsWithCmpOp = StringComparison.CurrentCulture;
                    if (args.Length == 2)
                    {
                        startsWithCmpOp = (StringComparison)args[1].Value;
                    }
                    switch (startsWithCmpOp)
                    {
                        case StringComparison.Ordinal:
                        case StringComparison.CurrentCulture:
                            sqlCall = "( substr(" + obj.CommandText + ", 1, " + args[0].Value.ToString().Length + ") = " + args[0].CommandText + ")";
                            break;
                        case StringComparison.OrdinalIgnoreCase:
                        case StringComparison.CurrentCultureIgnoreCase:
                            sqlCall = "(" + obj.CommandText + " like (" + args[0].CommandText + " || '%'))";
                            break;
                    }
                }
                else if (call.Method.Name == "EndsWith" && args.Length == 1)
                {
                    var endsWithCmpOp = StringComparison.CurrentCulture;
                    if (args.Length == 2)
                    {
                        endsWithCmpOp = (StringComparison)args[1].Value;
                    }
                    switch (endsWithCmpOp)
                    {
                        case StringComparison.Ordinal:
                        case StringComparison.CurrentCulture:
                            sqlCall = "( substr(" + obj.CommandText + ", length(" + obj.CommandText + ") - " + args[0].Value.ToString().Length + "+1, " + args[0].Value.ToString().Length + ") = " + args[0].CommandText + ")";
                            break;
                        case StringComparison.OrdinalIgnoreCase:
                        case StringComparison.CurrentCultureIgnoreCase:
                            sqlCall = "(" + obj.CommandText + " like ('%' || " + args[0].CommandText + "))";
                            break;
                    }
                }
                else if (call.Method.Name == "Equals" && args.Length == 1)
                {
                    sqlCall = "(" + obj.CommandText + " = (" + args[0].CommandText + "))";
                }
                else if (call.Method.Name == "ToLower")
                {
                    sqlCall = "(lower(" + obj.CommandText + "))";
                }
                else if (call.Method.Name == "ToUpper")
                {
                    sqlCall = "(upper(" + obj.CommandText + "))";
                }
                else if (call.Method.Name == "Replace" && args.Length == 2)
                {
                    sqlCall = "(replace(" + obj.CommandText + "," + args[0].CommandText + "," + args[1].CommandText + "))";
                }
                else
                {
                    sqlCall = call.Method.Name.ToLower() + "(" + string.Join(",", args.Select(a => a.CommandText)) + ")";
                }
                return new CompileResult
                {
                    CommandText = sqlCall,
                };
            }
            else if (expr.NodeType == ExpressionType.Constant)
            {
                var c = (ConstantExpression)expr;
                queryArgs.Add(c.Value);
                return new CompileResult
                {
                    CommandText = "@p" + queryArgs.Count.ToString(),
                    Value = c.Value,
                };
            }
            else if (expr.NodeType == ExpressionType.Convert)
            {
                var u = (UnaryExpression)expr;
                var ty = u.Type;
                var valr = CompileExpr(u.Operand, queryArgs);
                return new CompileResult
                {
                    CommandText = valr.CommandText,
                    Value = valr.Value != null ? ConvertTo(valr.Value, ty) : null,
                };
            }
            else if (expr.NodeType == ExpressionType.MemberAccess)
            {
                var mem = (MemberExpression)expr;

                var paramExpr = mem.Expression as ParameterExpression;
                if (paramExpr == null)
                {
                    var convert = mem.Expression as UnaryExpression;
                    if (convert != null && convert.NodeType == ExpressionType.Convert)
                    {
                        paramExpr = convert.Operand as ParameterExpression;
                    }
                }

                if (paramExpr != null)
                {
                    var columnName = Table.FindColumnWithPropertyName(mem.Member.Name).Name;
                    return new CompileResult
                    {
                        //CommandText = $"\"{columnName}\"",
                        CommandText = $"[{columnName}]",
                    };
                }
                else
                {
                    object obj = null;
                    if (mem.Expression != null)
                    {
                        var r = CompileExpr(mem.Expression, queryArgs);
                        if (r.Value == null)
                        {
                            throw new NotSupportedException(
                                "Member access failed to compile expression");
                        }
                        if (r.CommandText == ("@p" + queryArgs.Count.ToString()))
                        {
                            queryArgs.RemoveAt(queryArgs.Count - 1);
                        }
                        obj = r.Value;
                    }

                    // Get the member value
                    object val = null;
                    if (mem.Member is PropertyInfo)
                    {
                        var m = (PropertyInfo)mem.Member;
                        val = m.GetValue(obj, null);
                    }
                    else if (mem.Member is FieldInfo)
                    {
                        var m = (FieldInfo)mem.Member;
                        val = m.GetValue(obj);
                    }
                    else
                    {
                        throw new NotSupportedException(
                            "MemberExpr:" + mem.Member.GetType());
                    }

                    // Work special magic for enumerables
                    if (val != null && val is IEnumerable && !(val is string) && !(val is IEnumerable<byte>))
                    {
                        var sb = new StringBuilder();
                        sb.Append("(");
                        var head = "";
                        foreach (var a in (IEnumerable)val)
                        {
                            queryArgs.Add(a);
                            sb.Append(head);
                            sb.Append("@p" + queryArgs.Count.ToString());
                            head = ",";
                        }
                        sb.Append(")");
                        return new CompileResult
                        {
                            CommandText = sb.ToString(),
                            Value = val,
                        };
                    }
                    else
                    {
                        queryArgs.Add(val);
                        return new CompileResult
                        {
                            CommandText = "@p" + queryArgs.Count.ToString(),
                            Value = val,
                        };
                    }
                }
            }
            throw new NotSupportedException(
                $"Cannot compile: {expr.NodeType}");
        }

        static object ConvertTo(object obj, Type t)
        {
            var nut = Nullable.GetUnderlyingType(t);
            if (nut != null)
            {
                if (obj == null)
                {
                    return null;
                }
                else
                {
                    return Convert.ChangeType(obj, nut);
                }
            }
            else
            {
                return Convert.ChangeType(obj, t);
            }
        }

        static string CompileNullBinaryExpression(BinaryExpression expression, CompileResult parameter)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                    return "(" + parameter.CommandText + " is ?)";
                case ExpressionType.NotEqual:
                    return "(" + parameter.CommandText + " is not ?)";
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                    return "(" + parameter.CommandText + " < ?)";
                default:
                    throw new NotSupportedException(
                        $"Cannot compile Null-BinaryExpression with type {expression.NodeType}");
            }
        }

        static string GetSqlName(Expression expr)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.And:
                    return "&";
                case ExpressionType.AndAlso:
                    return "and";
                case ExpressionType.Or:
                    return "|";
                case ExpressionType.OrElse:
                    return "or";
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "!=";
                default:
                    throw new NotSupportedException(
                        $"Cannot get SQL for: {expr.NodeType}");
            }
        }

        public int Count()
        {
            using (var command = GenerateCommand("count(*)"))
            {
                return (int)command.ExecuteScalar();
            }
        }

        public int Count(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).Count();
        }

        public async Task<int> CountAsync()
        {
            using (var command = GenerateCommand("count(*)"))
            {
                return (int)await command.ExecuteScalarAsync();
            }
        }

        public Task<int> CountAsync(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).CountAsync();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return ToList().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public T First()
        {
            var query = Take(1);
            return query.ToList().First();
        }

        public T First(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).First();
        }

        public T FirstOrDefault()
        {
            var query = Take(1);
            return query.ToList().FirstOrDefault();
        }

        public T FirstOrDefault(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).FirstOrDefault();
        }

        public async Task<T> FirstAsync()
        {
            var list = await Take(1).ToListAsync();
            return list.First();
        }

        public async Task<T> FirstOrDefaultAsync()
        {
            var list = await Take(1).ToListAsync();
            return list.FirstOrDefault();
        }

        public Task<T> FirstAsync(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).FirstAsync();
        }

        public Task<T> FirstOrDefaultAsync(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).FirstOrDefaultAsync();
        }
    }
}
