using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Kuery
{
    /// <summary>Provides connection extensions for mapped tables, LINQ queries, and SQL commands.</summary>
    public static partial class SqlHelper
    {
        /// <summary>Creates a composable table query for the mapped type.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <returns>A table query that executes against the supplied connection.</returns>
        public static TableQuery<T> Table<T>(this IDbConnection connection)
        {
            var map = SqlMapper.GetMapping<T>();
            return new TableQuery<T>(connection, map);
        }

        /// <summary>Creates a deferred Kuery LINQ query for a mapped table.</summary>
        /// <typeparam name="T">The mapped element type.</typeparam>
        /// <param name="connection">The connection used when the query executes.</param>
        /// <returns>A query that translates supported LINQ operations to SQL.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="connection"/> is null.</exception>
        /// <remarks>Open the connection before executing or enumerating the query.
        /// Joined query sources must use the same connection. Unsupported expressions throw when the query is translated.</remarks>
        public static IQueryable<T> Query<T>(this IDbConnection connection)
        {
            Requires.NotNull(connection, nameof(connection));
            var provider = new KueryQueryProvider(connection);
            return new KueryQueryable<T>(provider);
        }

        /// <summary>Deletes rows selected by a filtered Kuery query.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="query">The Kuery query that selects the rows to modify.</param>
        /// <returns>The number of rows deleted.</returns>
        /// <exception cref="NotSupportedException">The provider or query shape does not support this operation.</exception>
        /// <exception cref="InvalidOperationException">No filter is specified.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> is null.</exception>
        /// <remarks>The query must target a single mapped table with a filter.</remarks>
        public static int ExecuteDelete<T>(this IQueryable<T> query)
        {
            Requires.NotNull(query, nameof(query));

            if (TryGetKueryProvider(query, out var provider))
            {
                return provider.ExecuteDelete(query.Expression);
            }

            throw new NotSupportedException("ExecuteDelete is supported only for queries created by Kuery Query<T>().");
        }

        /// <summary>Updates the specified properties of rows selected by a filtered Kuery query.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="query">The Kuery query that selects the rows to modify.</param>
        /// <param name="setPropertyCalls">A function that builds the property assignments for the update.</param>
        /// <returns>The number of rows updated.</returns>
        /// <exception cref="NotSupportedException">The provider or query shape does not support this operation.</exception>
        /// <exception cref="InvalidOperationException">No filter or property assignments are specified.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="query"/> or <paramref name="setPropertyCalls"/> is null.</exception>
        /// <remarks>The query must target a single mapped table with a filter.</remarks>
        public static int ExecuteUpdate<T>(
            this IQueryable<T> query,
            Func<SetPropertyCalls<T>, SetPropertyCalls<T>> setPropertyCalls)
        {
            Requires.NotNull(query, nameof(query));
            Requires.NotNull(setPropertyCalls, nameof(setPropertyCalls));

            var setters = setPropertyCalls(new SetPropertyCalls<T>());
            Requires.NotNull(setters, nameof(setPropertyCalls));

            if (TryGetKueryProvider(query, out var provider))
            {
                return provider.ExecuteUpdate(query.Expression, setters.Setters);
            }

            throw new NotSupportedException("ExecuteUpdate is supported only for queries created by Kuery Query<T>().");
        }

        /// <summary>Inserts an entity using its mapped column values.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="item">The entity whose mapped values are used by the operation.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The number of rows affected by the insert.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> is null.</exception>
        /// <remarks>A generated primary key is assigned back to the entity when configured by the mapping.</remarks>
        public static int Insert<T>(this IDbConnection connection, T item, IDbTransaction transaction = null)
        {
            return connection.Insert(typeof(T), item, transaction);
        }

        /// <summary>Inserts an entity using its mapped column values.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="item">The entity whose mapped values are used by the operation.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The number of rows affected by the insert.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> or <paramref name="type"/> is null.</exception>
        /// <remarks>A generated primary key is assigned back to the entity when configured by the mapping.</remarks>
        public static int Insert(this IDbConnection connection, Type type, object item, IDbTransaction transaction = null)
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
                count = command.ExecuteNonQuery();
            }

            if (map.HasAutoIncPK)
            {
                var id = connection.GetLastRowId(transaction);
                map.SetAutoIncPk(item, id);
            }

            return count;
        }

        private static long GetLastRowId(this IDbConnection connection, IDbTransaction transaction = null)
        {
            using (var command = connection.CreateLastInsertRowIdCommand())
            {
                command.Transaction = transaction;
                var result = command.ExecuteScalar();
                if (result is decimal d)
                {
                    return (long)d;
                }
                // MySQL may return UInt64 for LAST_INSERT_ID(), so use conversion instead of direct cast.
                return Convert.ToInt64(result);
            }
        }

        /// <summary>Inserts each entity in the supplied sequence.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="items">The entities to process, in enumeration order.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The total number of rows affected by the inserts.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="items"/> is null.</exception>
        /// <remarks>The runtime type of each entity determines its mapping. Supply a transaction to group the individual commands into one transaction.</remarks>
        public static int InsertAll(this IDbConnection connection, IEnumerable items, IDbTransaction transaction = null)
        {
            Requires.NotNull(items, nameof(items));

            var result = 0;
            foreach (var item in items)
            {
                result += connection.Insert(Orm.GetType(item), item, transaction);
            }
            return result;
        }

        /// <summary>Inserts each entity in the supplied sequence.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="items">The entities to process, in enumeration order.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The total number of rows affected by the inserts.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="items"/> or <paramref name="type"/> is null.</exception>
        /// <remarks>Supply a transaction to group the individual commands into one transaction.</remarks>
        public static int InsertAll(this IDbConnection connection, Type type, IEnumerable items, IDbTransaction transaction = null)
        {
            Requires.NotNull(items, nameof(items));
            Requires.NotNull(type, nameof(type));

            var result = 0;
            foreach (var item in items)
            {
                result += connection.Insert(type, item, transaction);
            }
            return result;
        }


        /// <summary>Updates the row identified by the entity’s primary key.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="item">The entity whose mapped values are used by the operation.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The number of rows affected by the update.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> is null.</exception>
        public static int Update<T>(this IDbConnection connection, T item, IDbTransaction transaction = null)
        {
            Requires.NotNull(item, nameof(item));

            return connection.Update(typeof(T), item, transaction);
        }

        /// <summary>Updates the row identified by the entity’s primary key.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="item">The entity whose mapped values are used by the operation.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The number of rows affected by the update.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> or <paramref name="type"/> is null.</exception>
        public static int Update(this IDbConnection connection, Type type, object item, IDbTransaction transaction = null)
        {
            Requires.NotNull(item, nameof(item));
            Requires.NotNull(type, nameof(type));

            using (var command = connection.CreateUpdateCommand(item, type))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQuery();
            }
        }

        /// <summary>Updates each supplied entity by its mapped primary key.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="items">The entities to process, in enumeration order.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The total number of rows affected by the updates.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="items"/> is null.</exception>
        /// <remarks>The runtime type of each entity determines its mapping. Supply a transaction to group the individual commands into one transaction.</remarks>
        public static int UpdateAll(this IDbConnection connection, IEnumerable items, IDbTransaction transaction = null)
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
                    result += command.ExecuteNonQuery();
                }
            }
            return result;
        }

        /// <summary>Inserts or replaces a row using the database provider’s conflict handling.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="item">The entity to insert or replace. A null entity leaves the table unchanged.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The number of rows affected, or zero if the entity is null.</returns>
        public static int InsertOrReplace(this IDbConnection connection, object item, IDbTransaction transaction = null)
        {
            if (item == null)
            {
                return 0;
            }
            return connection.InsertOrReplace(Orm.GetType(item), item, transaction);
        }

        /// <summary>Inserts or replaces a row using the database provider’s conflict handling.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="item">The entity to insert or replace. A null entity leaves the table unchanged.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The number of rows affected, or zero if the entity is null.</returns>
        public static int InsertOrReplace(this IDbConnection connection, Type type, object item, IDbTransaction transaction = null)
        {
            if (item == null)
            {
                return 0;
            }
            using (var command = connection.CreateInsertOrReplaceCommand(item, type))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQuery();
            }
        }

        /// <summary>Deletes the row identified by the entity’s primary key.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="item">The entity whose mapped values are used by the operation.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The number of rows deleted.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> is null.</exception>
        public static int Delete<T>(this IDbConnection connection, T item, IDbTransaction transaction = null)
        {
            Requires.NotNull(item, nameof(item));

            using (var command = connection.CreateDeleteCommand(item, typeof(T)))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQuery();
            }
        }

        /// <summary>Deletes a row by its primary key.</summary>
        /// <typeparam name="T">The mapped CLR type of the affected entity.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="primaryKey">The primary key value of the row to delete.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The number of rows deleted.</returns>
        public static int Delete<T>(this IDbConnection connection, object primaryKey, IDbTransaction transaction = null)
        {
            var map = SqlMapper.GetMapping<T>();
            return connection.Delete(map, primaryKey, transaction);
        }

        /// <summary>Deletes a row by its primary key.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="primaryKey">The primary key value of the row to delete.</param>
        /// <param name="transaction">The transaction to associate with each command, if any.</param>
        /// <returns>The number of rows deleted.</returns>
        public static int Delete(this IDbConnection connection, Type type, object primaryKey, IDbTransaction transaction = null)
        {
            var map = SqlMapper.GetMapping(type);
            return connection.Delete(map, primaryKey, transaction);
        }

        private static int Delete(this IDbConnection connection, TableMapping map, object primaryKey, IDbTransaction transaction = null)
        {
            Requires.NotNull(map, nameof(map));

            using (var command = connection.CreateDeleteCommand(primaryKey, map))
            {
                command.Transaction = transaction;
                return command.ExecuteNonQuery();
            }
        }


        /// <summary>Finds a row by primary key.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="pk">The primary key value to look up.</param>
        /// <returns>The matching entity, or the default value if no row matches.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="pk"/> is null.</exception>
        public static T Find<T>(this IDbConnection connection, object pk)
        {
            Requires.NotNull(pk, nameof(pk));

            var map = SqlMapper.GetMapping(typeof(T));
            return connection.Find<T>(map, pk);
        }

        private static T Find<T>(this IDbConnection connection, TableMapping mapping, object pk)
        {
            Requires.NotNull(pk, nameof(pk));
            Requires.NotNull(mapping, nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                return ExecuteQueryFirstOrDefault<T>(command, mapping);
            }
        }

        /// <summary>Finds the first row matching a predicate.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="predicate">The filter expression to evaluate against the mapped rows.</param>
        /// <returns>The matching entity, or the default value if no row matches.</returns>
        public static T Find<T>(this IDbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().FirstOrDefault(predicate);
        }

        /// <summary>Gets the first row matching a predicate.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="predicate">The filter expression to evaluate against the mapped rows.</param>
        /// <returns>The matching entity.</returns>
        /// <exception cref="InvalidOperationException">No row matches the supplied key or predicate.</exception>
        public static T Get<T>(this IDbConnection connection, Expression<Func<T, bool>> predicate)
        {
            return connection.Table<T>().First(predicate);
        }

        /// <summary>Gets the row with the specified primary key.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="pk">The primary key value to look up.</param>
        /// <returns>The matching entity.</returns>
        /// <exception cref="InvalidOperationException">No row matches the supplied key or predicate.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="pk"/> is null.</exception>
        public static T Get<T>(this IDbConnection connection, object pk)
        {
            Requires.NotNull(pk, nameof(pk));

            var map = SqlMapper.GetMapping(typeof(T));
            return connection.Get<T>(map, pk);
        }

        /// <summary>Gets the row with the specified primary key.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="pk">The primary key value to look up.</param>
        /// <returns>The matching entity.</returns>
        /// <exception cref="InvalidOperationException">No row matches the supplied key or predicate.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="pk"/> is null.</exception>
        public static T Get<T>(this IDbConnection connection, Type type, object pk)
        {
            var map = SqlMapper.GetMapping(type);
            return connection.Get<T>(map, pk);
        }

        private static T Get<T>(this IDbConnection connection, TableMapping mapping, object pk)
        {
            Requires.NotNull(pk, nameof(pk));
            Requires.NotNull(mapping, nameof(mapping));

            using (var command = connection.CreateGetByPrimaryKeyCommand(mapping, pk))
            {
                var result = ExecuteQuery<T>(command, mapping);
                return result.First();
            }
        }

        /// <summary>Executes a command and buffers all of its mapped rows.</summary>
        /// <typeparam name="T">The mapped CLR type to read from the result.</typeparam>
        /// <param name="command">The database command to execute.</param>
        /// <param name="map">The mapping that associates CLR properties with database columns.</param>
        /// <returns>The materialized rows.</returns>
        internal static List<T> ExecuteQuery<T>(this IDbCommand command, TableMapping map)
        {
            using (var reader = command.ExecuteReader())
            {
                return reader.ToList<T>(map);
            }
        }

        /// <summary>Executes a command and reads its first mapped row.</summary>
        /// <typeparam name="T">The mapped CLR type to read from the result.</typeparam>
        /// <param name="command">The database command to execute.</param>
        /// <param name="map">The mapping that associates CLR properties with database columns.</param>
        /// <returns>The first mapped row, or the default value if the reader is empty.</returns>
        internal static T ExecuteQueryFirstOrDefault<T>(this IDbCommand command, TableMapping map)
        {
            using (var reader = command.ExecuteReader())
            {
                return reader.FirstOrDefault<T>(map);
            }
        }

        /// <summary>Executes a parameterized SQL query and maps its result columns.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <returns>The materialized rows, in the order returned by the database.</returns>
        public static IEnumerable<T> Query<T>(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var map = SqlMapper.GetMapping<T>();
                return ExecuteQuery<T>(command, map);
            }
        }

        /// <summary>Executes a parameterized SQL query and maps its result columns.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <returns>The materialized rows, in the order returned by the database.</returns>
        public static IEnumerable<object> Query(this IDbConnection connection, Type type, string sql, object param = null)
        {
            var map = SqlMapper.GetMapping(type);
            return connection.Query(map, sql, param);
        }

        private static IEnumerable<object> Query(this IDbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            Requires.NotNull(mapping, nameof(mapping));

            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQuery<object>(command, mapping);
            }
        }

        /// <summary>Executes a parameterized SQL query and maps its first row.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <returns>The first mapped result, or the default value if the query is empty.</returns>
        public static T FindWithQuery<T>(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var map = SqlMapper.GetMapping<T>();
                return ExecuteQueryFirstOrDefault<T>(command, map);
            }
        }

        /// <summary>Executes a parameterized SQL query and maps its first row.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="type">The CLR type whose mapping is used for the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <returns>The first mapped result, or the default value if the query is empty.</returns>
        public static object FindWithQuery(this IDbConnection connection, Type type, string sql, object param = null)
        {
            var map = SqlMapper.GetMapping(type);
            return connection.FindWithQuery(map, sql, param);
        }

        private static object FindWithQuery(this IDbConnection connection, TableMapping mapping, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return ExecuteQueryFirstOrDefault<object>(command, mapping);
            }
        }

        /// <summary>Executes a parameterized SQL command without reading a result set.</summary>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <returns>The affected row count reported by the provider.</returns>
        public static int Execute(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                return command.ExecuteNonQuery();
            }
        }

        /// <summary>Executes a parameterized SQL command and reads its first column from the first row.</summary>
        /// <typeparam name="T">The CLR type to which the scalar result is converted.</typeparam>
        /// <param name="connection">The database connection used by the operation.</param>
        /// <param name="sql">The SQL command text to execute.</param>
        /// <param name="param">Optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <returns>The scalar converted to the requested type, or the default value if the command returns no value or NULL.</returns>
        public static T ExecuteScalar<T>(this IDbConnection connection, string sql, object param = null)
        {
            using (var command = connection.CreateParameterizedCommand(sql, param))
            {
                var result = command.ExecuteScalar();
                if (result is null || result is DBNull)
                {
                    return default;
                }
                var clrType = typeof(T);
                if (clrType.IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    clrType = typeof(T).GetGenericArguments()[0];
                }
                return (T)Convert.ChangeType(result, clrType);
            }
        }
    }

    /// <summary>Specifies the database table name for a mapped type.</summary>
    [AttributeUsage(AttributeTargets.Class)]
    public sealed class TableAttribute : Attribute
    {
        /// <summary>Gets the database table name.</summary>
        public string Name { get; }

        /// <summary>Gets or sets whether the table mapping requests SQLite's WITHOUT ROWID option.</summary>
        public bool WithoutRowId { get; set; }

        /// <summary>Initializes a table mapping attribute with the specified name.</summary>
        /// <param name="name">The database table name.</param>
        public TableAttribute(string name)
        {
            Name = name;
        }
    }

    /// <summary>Specifies the database column name for a mapped property.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class ColumnAttribute : Attribute
    {
        /// <summary>Gets the database column name.</summary>
        public string Name { get; }

        /// <summary>Initializes a column mapping attribute with the specified name.</summary>
        /// <param name="name">The database column name.</param>
        public ColumnAttribute(string name)
        {
            Name = name;
        }
    }

    /// <summary>Marks a mapped property as the primary key.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class PrimaryKeyAttribute : Attribute
    {
    }

    /// <summary>Marks a mapped primary key for database-generated numeric values or generated GUIDs.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class AutoIncrementAttribute : Attribute
    {
    }

    /// <summary>Marks a mapped property as part of an index.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class IndexedAttribute : Attribute
    {
        /// <summary>Gets or sets the index name. Use the same name on multiple properties to describe a composite index.</summary>
        public string Name { get; set; }

        /// <summary>Gets or sets the position of the property within the index.</summary>
        public int Order { get; set; }

        /// <summary>Gets or sets whether the index requires unique values.</summary>
        public virtual bool Unique { get; set; }

        /// <summary>Initializes an index attribute.</summary>
        public IndexedAttribute()
        {
        }

        /// <summary>Initializes an index attribute for a named index with a specified column order.</summary>
        /// <param name="name">The name of the index to which the property belongs.</param>
        /// <param name="order">The position of the column within the index.</param>
        public IndexedAttribute(string name, int order)
        {
            Name = name;
            Order = order;
        }
    }

    /// <summary>Excludes a property from Kuery mapping.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class IgnoreAttribute : Attribute
    {
    }

    /// <summary>Marks a mapped property as part of a unique index.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class UniqueAttribute : IndexedAttribute
    {
        /// <summary>Gets a value that is always true. Setting this property has no effect.</summary>
        public override bool Unique
        {
            get => true;
            set { }
        }
    }

    /// <summary>Specifies the maximum length used when declaring a mapped string column.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MaxLengthAttribute : Attribute
    {
        /// <summary>Gets the declared maximum length of the mapped string column.</summary>
        public int Value { get; }

        /// <summary>Initializes a maximum length attribute.</summary>
        /// <param name="length">The maximum length to declare for the column.</param>
        public MaxLengthAttribute(int length)
        {
            Value = length;
        }
    }

    /// <summary>Describes which attributed code should be retained by a linker that recognizes this attribute.</summary>
    public sealed class PreserveAttribute : Attribute
    {
        /// <summary>Gets or sets whether all members of the attributed type should be retained.</summary>
        public bool AllMembers { get; set; }

        /// <summary>Gets or sets whether retention depends on the attributed code already being referenced.</summary>
        public bool Conditional { get; set; }
    }

    /// <summary>Specifies the collation used in the declaration of a mapped column.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class CollationAttribute : Attribute
    {
        /// <summary>Gets the database collation name.</summary>
        public string Value { get; }

        /// <summary>Initializes a collation attribute with a database collation name.</summary>
        /// <param name="collation">The collation name understood by the database.</param>
        public CollationAttribute(string collation)
        {
            Value = collation;
        }
    }

    /// <summary>Marks a mapped column as non-nullable.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class NotNullAttribute : Attribute
    {
    }

    /// <summary>Maps values of the attributed enum by name instead of as their underlying numbers.</summary>
    [AttributeUsage(AttributeTargets.Enum)]
    public sealed class StoreAsTextAttribute : Attribute
    {
    }

    /// <summary>Describes the database table and columns mapped to a CLR type.</summary>
    internal sealed class TableMapping
    {
        /// <summary>Gets the CLR type represented by this mapping.</summary>
        public Type MappedType { get; }

        /// <summary>Gets the mapped database table name.</summary>
        public string TableName { get; }

        /// <summary>Gets whether the mapping requests a SQLite table without a row ID.</summary>
        public bool WithoutRowId { get; }

        /// <summary>Gets the columns mapped for the CLR type.</summary>
        public IReadOnlyList<Column> Columns { get; }

        /// <summary>Gets the mapped primary key column, or null when no primary key is mapped.</summary>
        public Column PK { get; }

        /// <summary>Gets the flags used to infer mapping conventions.</summary>
        public CreateFlags CreateFlags { get; }

        private readonly Column _autoPk;

        private readonly IReadOnlyDictionary<string, Column> _lookupByColumnName;

        private readonly IReadOnlyDictionary<string, Column> _lookupByPropertyName;

        /// <summary>Builds the table, column, and key metadata from a CLR type and mapping conventions.</summary>
        /// <param name="type">The CLR type whose table and properties are mapped.</param>
        /// <param name="createFlags">The flags that control implicit mapping conventions.</param>
        internal TableMapping(
            Type type,
            CreateFlags createFlags = CreateFlags.None)
        {
            MappedType = type;
            CreateFlags = createFlags;

            var typeInfo = type.GetTypeInfo();
            var tableAttr = typeInfo.CustomAttributes
                .Where(x => Orm.IsTableAttribute(x.AttributeType))
                .FirstOrDefault();

            var tableName = tableAttr != null ? Orm.GetAttributeName(tableAttr) : null;
            TableName = !string.IsNullOrEmpty(tableName) ? tableName : MappedType.Name;
            WithoutRowId = tableAttr != null && Orm.GetWithoutRowId(tableAttr);

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
                    cols.Add(new Column(MappedType, p, createFlags));
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

            _lookupByColumnName = Columns.ToDictionary(x => x.Name.ToLower());
            _lookupByPropertyName = Columns.ToDictionary(x => x.PropertyName);

            HasAutoIncPK = _autoPk != null;

            InsertColumns = Columns.Where(c => !c.IsAutoInc).ToArray();
            InsertOrReplaceColumns = Columns.ToArray();
        }

        /// <summary>Gets whether the mapping has an auto-incrementing numeric primary key.</summary>
        public bool HasAutoIncPK { get; }

        /// <summary>Copies a generated numeric primary key to an entity when the mapping has one.</summary>
        /// <param name="obj">The entity that received the generated key.</param>
        /// <param name="id">The generated numeric primary key.</param>
        public void SetAutoIncPk(object obj, long id)
        {
            if (_autoPk != null)
            {
                _autoPk.SetValue(obj, Convert.ChangeType(id, _autoPk.ColumnType, null));
            }
        }

        /// <summary>Gets the columns included in a normal insert.</summary>
        public IReadOnlyList<Column> InsertColumns { get; }

        /// <summary>Gets the columns included in an insert-or-replace operation.</summary>
        public IReadOnlyList<Column> InsertOrReplaceColumns { get; }

        /// <summary>Finds the column mapped from the specified CLR property name.</summary>
        /// <param name="propertyName">The name of the mapped CLR property.</param>
        /// <returns>The column mapping, or null if the property is not mapped.</returns>
        public Column FindColumnWithPropertyName(string propertyName) =>
            _lookupByPropertyName.TryGetValue(propertyName, out var column) ? column : null;

        /// <summary>Finds the mapping for a database column name without regard to case.</summary>
        /// <param name="columnName">The database column name.</param>
        /// <returns>The column mapping, or null if the name is not mapped.</returns>
        public Column FindColumn(string columnName) =>
            _lookupByColumnName.TryGetValue(columnName.ToLower(), out var column) ? column : null;

        /// <summary>Describes a mapped property and how values for its database column are read and written.</summary>
        internal sealed class Column
        {
            private readonly PropertyInfo _prop;

            /// <summary>Gets the mapped database column name.</summary>
            public string Name { get; }

            /// <summary>Gets the reflection metadata for the mapped property.</summary>
            public PropertyInfo PropertyInfo => _prop;

            /// <summary>Gets the name of the mapped CLR property.</summary>
            public string PropertyName => _prop.Name;

            /// <summary>Gets the CLR type of the column value after removing any nullable wrapper.</summary>
            public Type ColumnType { get; }

            /// <summary>Gets the configured database collation, if any.</summary>
            public string Collation { get; }

            /// <summary>Gets whether the column uses an auto-incrementing numeric value.</summary>
            public bool IsAutoInc { get; }

            /// <summary>Gets whether a GUID value should be generated for the column.</summary>
            public bool IsAutoGuid { get; }

            /// <summary>Gets whether the column is the primary key.</summary>
            public bool IsPK { get; }

            /// <summary>Gets the configured index entries for the column.</summary>
            public IEnumerable<IndexedAttribute> Indices { get; }

            /// <summary>Gets whether the mapping permits a database NULL for this column.</summary>
            public bool IsNullable { get; }

            /// <summary>Gets the configured maximum string length, or null if none is declared.</summary>
            public int? MaxStringLength { get; }

            /// <summary>Gets whether the property value is represented as text in the database.</summary>
            public bool StoreAsText { get; }

            /// <summary>The CLR type whose mapping contains this column.</summary>
            internal readonly Type MappedType;

            /// <summary>The cached delegate that reads a column by ordinal and assigns the converted value to an entity.</summary>
            internal readonly Action<object, IDataRecord, int> FastSetter;

            /// <summary>Builds column metadata from a mapped property and the mapping flags.</summary>
            /// <param name="mappedType">The CLR type that owns the mapped property.</param>
            /// <param name="prop">The mapped CLR property.</param>
            /// <param name="createFlags">The flags that control implicit mapping conventions.</param>
            public Column(Type mappedType, PropertyInfo prop, CreateFlags createFlags = CreateFlags.None)
            {
                var colAttr = prop.CustomAttributes.FirstOrDefault(x => Orm.IsColumnAttribute(x.AttributeType));

                _prop = prop;
                MappedType = mappedType;
                Name = (colAttr != null ? Orm.GetAttributeName(colAttr) : null) ?? prop.Name;

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

                StoreAsText = ColumnType
                    .GetTypeInfo()
                    .CustomAttributes
                    .Any(x => x.AttributeType == typeof(StoreAsTextAttribute));

                FastSetter = Kuery.FastSetter.Typeless.GetFastSetter(this);
            }

            /// <summary>Converts and assigns a database value to the mapped property.</summary>
            /// <param name="obj">The entity instance containing the mapped property.</param>
            /// <param name="val">The database value to assign to the property.</param>
            public void SetValue(object obj, object val)
            {
                if (val == null || val == DBNull.Value)
                {
                    _prop.SetValue(
                        obj: obj,
                        value: null,
                        index: null);
                }
                else if (ColumnType.GetTypeInfo().IsEnum)
                {
                    if (val is string enumStr && StoreAsText)
                    {
                        _prop.SetValue(
                            obj: obj,
                            value: Enum.Parse(ColumnType, enumStr),
                            index: null);
                    }
                    else
                    {
                        _prop.SetValue(
                            obj: obj,
                            value: Enum.ToObject(ColumnType, val),
                            index: null);
                    }
                }
                else if ((_prop.PropertyType == typeof(TimeSpan) || _prop.PropertyType == typeof(TimeSpan?)) &&
                    val is string timeSpanStr &&
                    TimeSpan.TryParse(timeSpanStr, out var timespan))
                {
                    _prop.SetValue(
                        obj: obj,
                        value: timespan,
                        index: null);
                }
                else if ((_prop.PropertyType == typeof(DateTime) || _prop.PropertyType == typeof(DateTime?)) &&
                    val is string dateTimeStr &&
                    DateTime.TryParse(dateTimeStr, out var dateTime))
                {
                    _prop.SetValue(
                        obj: obj,
                        value: dateTime,
                        index: null);
                }
                else if ((_prop.PropertyType == typeof(DateTimeOffset) || _prop.PropertyType == typeof(DateTimeOffset?)) &&
                    val is string dateTimeOffsetStr &&
                    DateTimeOffset.TryParse(dateTimeOffsetStr, out var dateTimeOffset))
                {
                    _prop.SetValue(
                        obj: obj,
                        value: dateTimeOffset,
                        index: null);
                }
                else if ((_prop.PropertyType == typeof(Guid) || _prop.PropertyType == typeof(Guid?)) &&
                    val is string guidStr &&
                    Guid.TryParse(guidStr, out var guid))
                {
                    _prop.SetValue(
                        obj: obj,
                        value: guid,
                        index: null);
                }
                else if (_prop.PropertyType.IsGenericType && _prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    var clrType = _prop.PropertyType.GetGenericArguments()[0];
                    _prop.SetValue(
                        obj: obj,
                        value: Convert.ChangeType(val, clrType),
                        index: null);
                }
                else
                {
                    _prop.SetValue(
                        obj: obj,
                        value: Convert.ChangeType(val, _prop.PropertyType),
                        index: null);
                }
            }

            /// <summary>Reads the mapped property value from an entity.</summary>
            /// <param name="obj">The entity instance containing the mapped property.</param>
            /// <returns>The CLR value stored in the mapped property.</returns>
            public object GetValue(object obj)
            {
                return _prop.GetValue(obj, null);
            }
        }
    }

    /// <summary>Describes whether and how a CLR enum is mapped to text.</summary>
    sealed class EnumCacheInfo
    {
        /// <summary>Builds cached metadata about a CLR type's enum representation.</summary>
        /// <param name="type">The CLR type to inspect.</param>
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

        /// <summary>Gets whether the CLR type is an enum.</summary>
        public bool IsEnum { get; }

        /// <summary>Gets whether the enum is stored using member names.</summary>
        public bool StoreAsText { get; }

        /// <summary>Gets the cached enum names by numeric value, when available.</summary>
        public Dictionary<int, string> EnumValues { get; }
    }

    /// <summary>Caches enum metadata used during parameter binding and materialization.</summary>
    static class EnumCache
    {
        private static readonly Dictionary<Type, EnumCacheInfo> _cache = new Dictionary<Type, EnumCacheInfo>();

        /// <summary>Gets or creates the cached enum metadata for the requested CLR type.</summary>
        /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
        /// <returns>The cached enum metadata for the type.</returns>
        public static EnumCacheInfo GetInfo<T>()
        {
            return GetInfo(typeof(T));
        }

        /// <summary>Gets or creates the cached enum metadata for the requested CLR type.</summary>
        /// <param name="type">The CLR type to inspect.</param>
        /// <returns>The cached enum metadata for the type.</returns>
        public static EnumCacheInfo GetInfo(Type type)
        {
            lock (_cache)
            {
                if (!_cache.TryGetValue(type, out var info))
                {
                    info = new EnumCacheInfo(type);
                    _cache[type] = info;
                }
                return info;
            }
        }
    }

    /// <summary>Reads mapping conventions and attributes and resolves corresponding SQL metadata.</summary>
    static class Orm
    {
        /// <summary>The conventional default length for string columns.</summary>
        public const int DefaultMaxStringLength = 140;
        /// <summary>The property name used by the implicit primary key convention.</summary>
        public const string ImplicitPkName = "Id";
        /// <summary>The column name suffix used by the implicit index convention.</summary>
        public const string ImplicitIndexSuffix = "Id";
        private const string DataAnnotationsSchemaTableAttributeFullName = "System.ComponentModel.DataAnnotations.Schema.TableAttribute";
        private const string DataAnnotationsSchemaColumnAttributeFullName = "System.ComponentModel.DataAnnotations.Schema.ColumnAttribute";
        private const string DataAnnotationsRequiredAttributeFullName = "System.ComponentModel.DataAnnotations.RequiredAttribute";
        private const string DataAnnotationsMaxLengthAttributeFullName = "System.ComponentModel.DataAnnotations.MaxLengthAttribute";
        private const string DataAnnotationsStringLengthAttributeFullName = "System.ComponentModel.DataAnnotations.StringLengthAttribute";

        /// <summary>Gets the runtime CLR type of an object, or System.Object for a null value.</summary>
        /// <param name="obj">The instance whose runtime type is requested.</param>
        /// <returns>The runtime CLR type, or System.Object if the instance is null.</returns>
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

        /// <summary>Builds the SQL column declaration, including the applicable constraints.</summary>
        /// <param name="p">The mapped column to describe.</param>
        /// <param name="storeDateTimeAsTicks">Whether to store date and time values as ticks.</param>
        /// <param name="storeTimeSpanAsTicks">Whether to store duration values as ticks.</param>
        /// <returns>The SQL declaration for the column.</returns>
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

        /// <summary>Maps a column's CLR type and storage options to a SQL type name.</summary>
        /// <param name="p">The mapped column to describe.</param>
        /// <param name="storeDateTimeAsTicks">Whether to store date and time values as ticks.</param>
        /// <param name="storeTimeSpanAsTicks">Whether to store duration values as ticks.</param>
        /// <returns>The database type name.</returns>
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

        /// <summary>Checks whether the property is marked as a primary key.</summary>
        /// <param name="p">The mapped CLR member to inspect.</param>
        /// <returns>True if the member has the primary key attribute.</returns>
        public static bool IsPK(MemberInfo p)
        {
            return p.CustomAttributes.Any(x => x.AttributeType == typeof(PrimaryKeyAttribute));
        }

        /// <summary>Gets the collation declared on a member, if any.</summary>
        /// <param name="p">The mapped CLR member to inspect.</param>
        /// <returns>The declared collation name, or null when the member has none.</returns>
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

        /// <summary>Checks whether a member is marked for auto-incrementing values.</summary>
        /// <param name="p">The mapped CLR member to inspect.</param>
        /// <returns>True if the member has the auto-increment attribute.</returns>
        public static bool IsAutoInc(MemberInfo p)
        {
            return p.CustomAttributes.Any(x => x.AttributeType == typeof(AutoIncrementAttribute));
        }

        /// <summary>Finds a named field declared on a type or one of its base types.</summary>
        /// <param name="t">The reflection metadata for the type where the search begins.</param>
        /// <param name="name">The name of the member to find.</param>
        /// <returns>The matching field.</returns>
        public static FieldInfo GetField(TypeInfo t, string name)
        {
            var f = t.GetDeclaredField(name);
            if (f != null)
            {
                return f;
            }
            return GetField(t.BaseType.GetTypeInfo(), name);
        }

        /// <summary>Finds a named property declared on a type or one of its base types.</summary>
        /// <param name="t">The reflection metadata for the type where the search begins.</param>
        /// <param name="name">The name of the member to find.</param>
        /// <returns>The matching property.</returns>
        public static PropertyInfo GetProperty(TypeInfo t, string name)
        {
            var f = t.GetDeclaredProperty(name);
            if (f != null)
            {
                return f;
            }
            return GetProperty(t.BaseType.GetTypeInfo(), name);
        }

        /// <summary>Constructs an attribute instance and sets the values from its metadata.</summary>
        /// <param name="x">The metadata used to construct the attribute instance.</param>
        /// <returns>The constructed attribute instance.</returns>
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

        /// <summary>Checks whether a type is a recognized table mapping attribute.</summary>
        /// <param name="attributeType">The attribute type to inspect.</param>
        /// <returns>True for a supported table mapping attribute type.</returns>
        public static bool IsTableAttribute(Type attributeType) =>
            attributeType == typeof(TableAttribute) ||
            string.Equals(attributeType.FullName, DataAnnotationsSchemaTableAttributeFullName, StringComparison.Ordinal);

        /// <summary>Checks whether a type is a recognized column mapping attribute.</summary>
        /// <param name="attributeType">The attribute type to inspect.</param>
        /// <returns>True for a supported column mapping attribute type.</returns>
        public static bool IsColumnAttribute(Type attributeType) =>
            attributeType == typeof(ColumnAttribute) ||
            string.Equals(attributeType.FullName, DataAnnotationsSchemaColumnAttributeFullName, StringComparison.Ordinal);

        private static bool IsRequiredAttribute(Type attributeType) =>
            string.Equals(attributeType.FullName, DataAnnotationsRequiredAttributeFullName, StringComparison.Ordinal);

        private static bool IsDataAnnotationsMaxLengthAttribute(Type attributeType) =>
            string.Equals(attributeType.FullName, DataAnnotationsMaxLengthAttributeFullName, StringComparison.Ordinal);

        private static bool IsDataAnnotationsStringLengthAttribute(Type attributeType) =>
            string.Equals(attributeType.FullName, DataAnnotationsStringLengthAttributeFullName, StringComparison.Ordinal);

        /// <summary>Reads the name passed to a recognized table or column attribute.</summary>
        /// <param name="attribute">The attribute metadata to inspect.</param>
        /// <returns>The configured name, or null when it cannot be read.</returns>
        public static string GetAttributeName(CustomAttributeData attribute)
        {
            if (attribute == null)
            {
                return null;
            }

            if (!IsTableAttribute(attribute.AttributeType) && !IsColumnAttribute(attribute.AttributeType))
            {
                return null;
            }

            if (attribute.ConstructorArguments.Count == 0)
            {
                return null;
            }

            var nameArgument = attribute.ConstructorArguments[0];
            if (!(nameArgument.Value is string name))
            {
                return null;
            }
            return name;
        }

        /// <summary>Reads the SQLite WITHOUT ROWID option from the attribute metadata.</summary>
        /// <param name="attribute">The attribute metadata to inspect.</param>
        /// <returns>True if the attribute enables WITHOUT ROWID.</returns>
        public static bool GetWithoutRowId(CustomAttributeData attribute)
        {
            foreach (var namedArgument in attribute.NamedArguments)
            {
                if (namedArgument.MemberName == nameof(TableAttribute.WithoutRowId))
                {
                    return (namedArgument.TypedValue.Value as bool?) == true;
                }
            }
            return false;
        }

        private static int? GetLengthFromAttribute(CustomAttributeData attr)
        {
            if (attr.ConstructorArguments.Count > 0)
            {
                var value = attr.ConstructorArguments[0].Value;
                if (value is int intValue)
                {
                    return intValue;
                }
            }
            return null;
        }

        /// <summary>Reads the index attributes declared on a member.</summary>
        /// <param name="p">The mapped CLR member to inspect.</param>
        /// <returns>The index definitions applied to the member.</returns>
        public static IEnumerable<IndexedAttribute> GetIndices(MemberInfo p)
        {
            var indexedInfo = typeof(IndexedAttribute).GetTypeInfo();
            return p.CustomAttributes
                .Where(x => indexedInfo.IsAssignableFrom(x.AttributeType.GetTypeInfo()))
                .Select(x => (IndexedAttribute)InflateAttribute(x));
        }

        /// <summary>Reads the maximum string length declared on a property.</summary>
        /// <param name="p">The mapped CLR member to inspect.</param>
        /// <returns>The declared length, or null if no supported length attribute is present.</returns>
        public static int? MaxStringLength(PropertyInfo p)
        {
            var attr = p.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(MaxLengthAttribute));
            if (attr != null)
            {
                var attrv = (MaxLengthAttribute)InflateAttribute(attr);
                return attrv.Value;
            }

            var dataAnnotationsMaxLengthAttr = p.CustomAttributes.FirstOrDefault(x => IsDataAnnotationsMaxLengthAttribute(x.AttributeType));
            if (dataAnnotationsMaxLengthAttr != null)
            {
                return GetLengthFromAttribute(dataAnnotationsMaxLengthAttr);
            }

            var dataAnnotationsStringLengthAttr = p.CustomAttributes.FirstOrDefault(x => IsDataAnnotationsStringLengthAttribute(x.AttributeType));
            if (dataAnnotationsStringLengthAttr != null)
            {
                return GetLengthFromAttribute(dataAnnotationsStringLengthAttr);
            }

            return null;
        }

        /// <summary>Checks for a recognized non-null or required attribute.</summary>
        /// <param name="p">The mapped CLR member to inspect.</param>
        /// <returns>True if the member is marked as non-nullable.</returns>
        public static bool IsMarkedNotNull(MemberInfo p)
        {
            return p.CustomAttributes.Any(x =>
                x.AttributeType == typeof(NotNullAttribute) ||
                IsRequiredAttribute(x.AttributeType));
        }
    }

    /// <summary>Controls implicit mapping conventions and table creation options.</summary>
    [Flags]
    internal enum CreateFlags
    {
        /// <summary>Uses explicit mapping attributes without enabling an implicit convention.</summary>
        None = 0x000,
        /// <summary>Treats the conventional Id property as the primary key.</summary>
        ImplicitPK = 0x001,
        /// <summary>Indexes columns whose names match the implicit index convention.</summary>
        ImplicitIndex = 0x002,
        /// <summary>Enables both implicit primary key and implicit index conventions.</summary>
        AllImplicit = 0x003,
        /// <summary>Requests automatic value generation for a mapped primary key.</summary>
        AutoIncPK = 0x004,
        /// <summary>Represents the SQLite FTS3 table option.</summary>
        FullTextSearch3 = 0x100,
        /// <summary>Represents the SQLite FTS4 table option.</summary>
        FullTextSearch4 = 0x200,
    }

    /// <summary>Provides the base for queries over mapped tables.</summary>
    public abstract class BaseTableQuery
    {
        /// <summary>Describes the direction and column for a table query sort key.</summary>
        protected class Ordering
        {
            /// <summary>Gets or sets the mapped column to sort by.</summary>
            public string ColumnName { get; set; }

            /// <summary>Gets or sets whether to sort in ascending order.</summary>
            public bool Ascending { get; set; }
        }
    }

    /// <summary>Represents a composable query over a mapped table.</summary>
    /// <typeparam name="T">The CLR type being queried or mapped.</typeparam>
    public sealed partial class TableQuery<T> : BaseTableQuery, IEnumerable<T>
    {
        /// <summary>Gets the connection used to execute the query.</summary>
        public IDbConnection Connection { get; }

        /// <summary>Gets the table mapping used to render SQL and read results.</summary>
        internal TableMapping Table { get; }

        private Expression _where;

        private List<Ordering> _orderBys;

        private int? _limit;

        private int? _offset;

        private BaseTableQuery _joinInner;

        private Expression _joinInnerKeySelector;

        private BaseTableQuery _joinOuter;

        private Expression _joinOuterKeySelector;

        private Expression _joinSelector;

        private Expression _selector;

        private bool _deferred;

        /// <summary>Creates a table query for a connection and a mapped entity type.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <param name="table">The table source or mapping associated with the operation.</param>
        internal TableQuery(IDbConnection connection, TableMapping table)
        {
            Connection = connection;
            Table = table;
        }

        /// <summary>Creates a table query using the cached mapping for its CLR type.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        internal TableQuery(IDbConnection connection)
        {
            Connection = connection;
            Table = SqlMapper.GetMapping(typeof(T));
        }

        private TableQuery<TOther> Clone<TOther>()
        {
            var q = new TableQuery<TOther>(Connection, Table);
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

        /// <summary>Combines a predicate with the existing query conditions.</summary>
        /// <param name="predExpr">The additional filter to combine with the existing query conditions.</param>
        /// <returns>A copy of the query with the operation applied.</returns>
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

        /// <summary>Deletes rows matching the combined query conditions.</summary>
        /// <returns>The number of rows deleted.</returns>
        /// <exception cref="InvalidOperationException">The query has a row limit or offset, or no filter is specified.</exception>
        public int Delete()
        {
            return Delete(null);
        }

        /// <summary>Deletes rows matching the combined query conditions.</summary>
        /// <param name="predExpr">An optional filter to combine with the existing query conditions.</param>
        /// <returns>The number of rows deleted.</returns>
        /// <exception cref="InvalidOperationException">The query has a row limit or offset, or no filter is specified.</exception>
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
            var cmdText = "delete from " + EscapeLiteral(Table.TableName);
            var w = CompileExpr(pred, args);
            cmdText += " where " + w.CommandText;

            using (var command = Connection.CreateCommand())
            {
                command.CommandText = cmdText;
                for (var i = 0; i < args.Count; i++)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = Connection.GetParameterName("p" + (i + 1).ToString());
                    parameter.Value = args[i];
                    command.Parameters.Add(parameter);
                }
                var result = command.ExecuteNonQuery();
                return result;
            }
        }

        /// <summary>Sets the maximum number of rows to return.</summary>
        /// <param name="n">The maximum number of rows to return.</param>
        /// <returns>A copy of the query with the operation applied.</returns>
        public TableQuery<T> Take(int n)
        {
            var q = Clone<T>();
            q._limit = n;
            return q;
        }

        /// <summary>Sets the number of rows to skip before returning results.</summary>
        /// <param name="n">The number of rows to skip.</param>
        /// <returns>A copy of the query with the operation applied.</returns>
        public TableQuery<T> Skip(int n)
        {
            var q = Clone<T>();
            q._offset = n;
            return q;
        }

        /// <summary>Returns the row at the specified zero-based offset.</summary>
        /// <param name="index">The zero-based row offset.</param>
        /// <returns>The row at the requested offset.</returns>
        /// <exception cref="InvalidOperationException">The query contains no row at the requested position.</exception>
        public T ElementAt(int index)
        {
            return Skip(index).Take(1).First();
        }

        /// <summary>Creates a copy with the deferred enumeration flag set.</summary>
        /// <returns>A copy of the query with the operation applied.</returns>
        public TableQuery<T> Deferred()
        {
            var q = Clone<T>();
            q._deferred = true;
            return q;
        }

        /// <summary>Adds an ascending sort key to the query.</summary>
        /// <typeparam name="TKey">The CLR type of the sort key.</typeparam>
        /// <param name="orderExpr">An expression selecting a mapped property to use as the sort key.</param>
        /// <returns>A copy of the query with the operation applied.</returns>
        /// <exception cref="NotSupportedException">The sort expression does not select a property directly.</exception>
        public TableQuery<T> OrderBy<TKey>(Expression<Func<T, TKey>> orderExpr)
        {
            return AddOrderBy(orderExpr, true);
        }

        /// <summary>Adds a descending sort key to the query.</summary>
        /// <typeparam name="TKey">The CLR type of the sort key.</typeparam>
        /// <param name="orderExpr">An expression selecting a mapped property to use as the sort key.</param>
        /// <returns>A copy of the query with the operation applied.</returns>
        /// <exception cref="NotSupportedException">The sort expression does not select a property directly.</exception>
        public TableQuery<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> orderExpr)
        {
            return AddOrderBy(orderExpr, false);
        }

        /// <summary>Adds an ascending secondary sort key to the query.</summary>
        /// <typeparam name="TKey">The CLR type of the sort key.</typeparam>
        /// <param name="orderExpr">An expression selecting a mapped property to use as the sort key.</param>
        /// <returns>A copy of the query with the operation applied.</returns>
        /// <exception cref="NotSupportedException">The sort expression does not select a property directly.</exception>
        public TableQuery<T> ThenBy<TKey>(Expression<Func<T, TKey>> orderExpr)
        {
            return AddOrderBy(orderExpr, true);
        }

        /// <summary>Adds a descending secondary sort key to the query.</summary>
        /// <typeparam name="U">The CLR type of the secondary sort key.</typeparam>
        /// <param name="orderExpr">An expression selecting a mapped property to use as the sort key.</param>
        /// <returns>A copy of the query with the operation applied.</returns>
        /// <exception cref="NotSupportedException">The sort expression does not select a property directly.</exception>
        public TableQuery<T> ThenByDescending<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, false);
        }

        TableQuery<T> AddOrderBy<TKey>(Expression<Func<T, TKey>> orderExpr, bool asc)
        {
            if (orderExpr.NodeType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)orderExpr;
                var unary = lambda.Body as UnaryExpression;

                MemberExpression mem;
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

        /// <summary>Executes the query and materializes the results in a list.</summary>
        /// <returns>A list containing the materialized rows.</returns>
        public List<T> ToList()
        {
            return GenerateCommand("*").ExecuteQuery<T>(Table);
        }

        /// <summary>Executes the query and materializes the results in an array.</summary>
        /// <returns>An array containing the materialized rows.</returns>
        public T[] ToArray()
        {
            return GenerateCommand("*").ExecuteQuery<T>(Table).ToArray();
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

        private IDbCommand GenerateCommand(string selectionList)
        {
            if (Connection.IsSqlServer())
            {
                return GenerateCommandForSqlServer(selectionList);
            }
            else if (Connection.IsPostgreSql())
            {
                return GenerateCommandForPostgreSql(selectionList);
            }
            else if (Connection.IsMySql())
            {
                return GenerateCommandForMySql(selectionList);
            }
            else
            {
                return GenerateCommandForSqlite(selectionList);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string EscapeLiteral(string literal) =>
            Connection.EscapeLiteral(literal);

        private IDbCommand GenerateCommandForPostgreSql(string selectionList)
        {
            if (_joinInner != null && _joinOuter != null)
            {
                throw new NotSupportedException("Joins are not supported.");
            }

            var commandText = new StringBuilder();
            commandText.Append("select ");
            commandText.Append(selectionList);
            commandText.Append(" from ");
            commandText.Append(EscapeLiteral(Table.TableName));
            commandText.Append(" ");

            List<object> args = null;
            if (_where != null)
            {
                args = new List<object>();
                var w = CompileExpr(_where, args);
                commandText.Append(" where ");
                commandText.Append(w.CommandText);
            }

            if (_orderBys != null && _orderBys.Count > 0)
            {
                commandText.Append(" order by ");
                commandText.Append(EscapeLiteral(_orderBys[0].ColumnName));
                if (!_orderBys[0].Ascending)
                {
                    commandText.Append(" desc ");
                }
                for (var i = 1; i < _orderBys.Count; i++)
                {
                    commandText.Append(", ");
                    commandText.Append(EscapeLiteral(_orderBys[i].ColumnName));
                    if (!_orderBys[i].Ascending)
                    {
                        commandText.Append(" desc ");
                    }
                }
            }

            if (_limit.HasValue)
            {
                commandText.Append(" limit ");
                commandText.Append(_limit.Value);
            }

            if (_offset.HasValue)
            {
                commandText.Append(" offset ");
                commandText.Append(_offset.Value);
            }

            var cmd = Connection.CreateCommand();
            cmd.CommandText = commandText.ToString();
            if (args != null)
            {
                for (var i = 0; i < args.Count; i++)
                {
                    var a = args[i];
                    if (a == null)
                    {
                        continue;
                    }
                    var p = cmd.CreateParameter();
                    p.Value = a;
                    p.ParameterName = GetParameterName("p" + (i + 1).ToString());
                    cmd.Parameters.Add(p);
                }
            }
            return cmd;
        }

        private IDbCommand GenerateCommandForSqlite(string selectionList)
        {
            if (_joinInner != null && _joinOuter != null)
            {
                throw new NotSupportedException("Joins are not supported.");
            }

            var commandText = new StringBuilder();
            commandText.Append("select ");
            commandText.Append(selectionList);
            commandText.Append(" from [");
            commandText.Append(Table.TableName);
            commandText.Append("] ");

            List<object> args = null;
            if (_where != null)
            {
                args = new List<object>();
                var w = CompileExpr(_where, args);
                commandText.Append(" where ");
                commandText.Append(w.CommandText);
            }

            if (_orderBys != null && _orderBys.Count > 0)
            {
                commandText.Append(" order by ");
                commandText.Append("[");
                commandText.Append(_orderBys[0].ColumnName);
                commandText.Append("]");
                if (!_orderBys[0].Ascending)
                {
                    commandText.Append(" desc ");
                }
                for (var i = 1; i < _orderBys.Count; i++)
                {
                    commandText.Append(", [");
                    commandText.Append(_orderBys[i].ColumnName);
                    commandText.Append("]");
                    if (!_orderBys[i].Ascending)
                    {
                        commandText.Append(" desc ");
                    }
                }
            }

            if (_limit.HasValue)
            {
                commandText.Append(" limit ");
                commandText.Append(_limit.Value);
            }

            if (_offset.HasValue)
            {
                if (!_limit.HasValue)
                {
                    commandText.Append(" limit -1 ");
                }
                commandText.Append(" offset ");
                commandText.Append(_offset.Value);
            }

            var cmd = Connection.CreateCommand();
            cmd.CommandText = commandText.ToString();
            if (args != null)
            {
                for (var i = 0; i < args.Count; i++)
                {
                    var a = args[i];
                    if (a == null)
                    {
                        continue;
                    }
                    var p = cmd.CreateParameter();
                    p.Value = a;
                    p.ParameterName = GetParameterName("p" + (i + 1).ToString());
                    cmd.Parameters.Add(p);
                }
            }
            return cmd;
        }

        private IDbCommand GenerateCommandForMySql(string selectionList)
        {
            if (_joinInner != null && _joinOuter != null)
            {
                throw new NotSupportedException("Joins are not supported.");
            }

            var commandText = new StringBuilder();
            commandText.Append("select ");
            commandText.Append(selectionList);
            commandText.Append(" from ");
            commandText.Append(EscapeLiteral(Table.TableName));
            commandText.Append(" ");

            List<object> args = null;
            if (_where != null)
            {
                args = new List<object>();
                var w = CompileExpr(_where, args);
                commandText.Append(" where ");
                commandText.Append(w.CommandText);
            }

            if (_orderBys != null && _orderBys.Count > 0)
            {
                commandText.Append(" order by ");
                commandText.Append(EscapeLiteral(_orderBys[0].ColumnName));
                if (!_orderBys[0].Ascending)
                {
                    commandText.Append(" desc ");
                }
                for (var i = 1; i < _orderBys.Count; i++)
                {
                    commandText.Append(", ");
                    commandText.Append(EscapeLiteral(_orderBys[i].ColumnName));
                    if (!_orderBys[i].Ascending)
                    {
                        commandText.Append(" desc ");
                    }
                }
            }

            if (_limit.HasValue)
            {
                commandText.Append(" limit ");
                commandText.Append(_limit.Value);
            }

            if (_offset.HasValue)
            {
                if (!_limit.HasValue)
                {
                    commandText.Append(" limit ");
                    commandText.Append(SqlConstants.MySqlOffsetWithoutLimitMax);
                }
                commandText.Append(" offset ");
                commandText.Append(_offset.Value);
            }

            var cmd = Connection.CreateCommand();
            cmd.CommandText = commandText.ToString();
            if (args != null)
            {
                for (var i = 0; i < args.Count; i++)
                {
                    var a = args[i];
                    if (a == null)
                    {
                        continue;
                    }
                    var p = cmd.CreateParameter();
                    p.Value = a;
                    p.ParameterName = GetParameterName("p" + (i + 1).ToString());
                    cmd.Parameters.Add(p);
                }
            }
            return cmd;
        }

        private IDbCommand GenerateCommandForSqlServer(string selectionList)
        {
            if (_joinInner != null && _joinOuter != null)
            {
                throw new NotSupportedException("Joins are not supported.");
            }

            var commandText = new StringBuilder();
            commandText.Append("select ");
            if (_limit.HasValue && !_offset.HasValue)
            {
                commandText.Append(" TOP (");
                commandText.Append(_limit.Value);
                commandText.Append(" ) ");
            }
            commandText.Append(selectionList);
            commandText.Append(" from [");
            commandText.Append(Table.TableName);
            commandText.Append("] ");

            List<object> args = null;
            if (_where != null)
            {
                args = new List<object>();
                var w = CompileExpr(_where, args);
                commandText.Append(" where ");
                commandText.Append(w.CommandText);
            }

            if (_orderBys != null && _orderBys.Count > 0)
            {
                commandText.Append(" order by ");
                commandText.Append("[");
                commandText.Append(_orderBys[0].ColumnName);
                commandText.Append("]");
                if (!_orderBys[0].Ascending)
                {
                    commandText.Append(" desc ");
                }
                for (var i = 1; i < _orderBys.Count; i++)
                {
                    commandText.Append(", [");
                    commandText.Append(_orderBys[i].ColumnName);
                    commandText.Append("]");
                    if (!_orderBys[i].Ascending)
                    {
                        commandText.Append(" desc ");
                    }
                }
            }

            if (_offset.HasValue)
            {
                if (_orderBys == null || _orderBys.Count == 0)
                {
                    commandText.Append(" order by ");
                    commandText.Append("[");
                    commandText.Append(Table.PK.Name);
                    commandText.Append("] ");
                }

                commandText.Append(" OFFSET ");
                commandText.Append(_offset.Value);
                commandText.Append(" ROWS ");

                if (_limit.HasValue)
                {
                    commandText.Append(" FETCH NEXT ");
                    commandText.Append(_limit.Value);
                    commandText.Append(" ROWS ONLY ");
                }
            }

            var cmd = Connection.CreateCommand();
            cmd.CommandText = commandText.ToString();
            if (args != null)
            {
                for (var i = 0; i < args.Count; i++)
                {
                    var a = args[i];
                    if (a == null)
                    {
                        continue;
                    }
                    var p = cmd.CreateParameter();
                    p.Value = a;
                    p.ParameterName = GetParameterName("p" + (i + 1).ToString());
                    cmd.Parameters.Add(p);
                }
            }
            return cmd;
        }

        private readonly struct CompileResult
        {
            /// <summary>Creates the compiled form of a table query expression.</summary>
            /// <param name="commandText">The SQL fragment generated for the expression.</param>
            /// <param name="value">The evaluated CLR value associated with the SQL fragment, if available.</param>
            public CompileResult(string commandText, object value = null)
            {
                CommandText = commandText;
                Value = value;
            }

            /// <summary>The SQL fragment produced for the expression.</summary>
            public readonly string CommandText;

            /// <summary>The evaluated value associated with the fragment, if available.</summary>
            public readonly object Value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GetParameterName(string name) =>
            Connection.GetParameterName(name);

        private CompileResult CompileExpr(Expression expr, List<object> queryArgs)
        {
            if (expr == null)
            {
                throw new NotSupportedException("Expression is NULL");
            }
            else if (expr is BinaryExpression)
            {
                var bin = (BinaryExpression)expr;
                var leftr = CompileExpr(bin.Left, queryArgs);
                var rightr = CompileExpr(bin.Right, queryArgs);

                string text;
                if (leftr.CommandText == GetParameterName("p" + queryArgs.Count.ToString()) && leftr.Value == null)
                {
                    text = CompileNullBinaryExpression(bin, rightr);
                }
                else if (rightr.CommandText == GetParameterName("p" + queryArgs.Count.ToString()) && rightr.Value == null)
                {
                    text = CompileNullBinaryExpression(bin, leftr);
                }
                else
                {
                    text = "(" + leftr.CommandText + " " + GetSqlName(bin) + " " + rightr.CommandText + ")";
                }
                return new CompileResult(commandText: text);
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
                return new CompileResult(
                    commandText: "not(" + opr.CommandText + ")",
                    value: val);
            }
            else if (expr.NodeType == ExpressionType.Call)
            {
                var call = (MethodCallExpression)expr;
                var args = new CompileResult[call.Arguments.Count];

                CompileResult? obj;
                if (call.Object != null)
                {
                    obj = CompileExpr(call.Object, queryArgs);
                }
                else
                {
                    obj = null;
                }

                for (var i = 0; i < args.Length; i++)
                {
                    args[i] = CompileExpr(call.Arguments[i], queryArgs);
                }

                var sqlCall = "";
                if (call.Method.Name == "Like" && args.Length == 2)
                {
                    sqlCall = "(" + args[0].CommandText + " like " + args[1].CommandText + ")";
                }
                else if (call.Method.Name == nameof(Enumerable.Contains) && args.Length == 2)
                {
                    sqlCall = "(" + args[1].CommandText + " in " + args[0].CommandText + ")";
                }
                else if (call.Method.Name == nameof(Enumerable.Contains) && args.Length == 1)
                {
                    if (call.Object != null && call.Object.Type == typeof(string))
                    {
                        if (Connection.IsSqlServer())
                        {
                            sqlCall = "( CHARINDEX(" + args[0].CommandText + "," + obj.Value.CommandText + ") > 0)";
                        }
                        else if (Connection.IsPostgreSql())
                        {
                            sqlCall = "( strpos(" + obj.Value.CommandText + "," + args[0].CommandText + ") > 0)";
                        }
                        else
                        {
                            sqlCall = "( instr(" + obj.Value.CommandText + "," + args[0].CommandText + ") > 0)";
                        }
                    }
                    else
                    {
                        sqlCall = "(" + args[0].CommandText + " in " + obj.Value.CommandText + ")";
                    }
                }
                else if (call.Method.Name == nameof(string.StartsWith) && args.Length >= 1)
                {
                    var startsWithCmpOp = StringComparison.CurrentCulture;
                    if (args.Length == 2)
                    {
                        if (args[1].CommandText == GetParameterName("p" + queryArgs.Count.ToString()))
                        {
                            queryArgs.RemoveAt(queryArgs.Count - 1);
                        }
                        startsWithCmpOp = (StringComparison)args[1].Value;
                    }
                    switch (startsWithCmpOp)
                    {
                        case StringComparison.Ordinal:
                        case StringComparison.CurrentCulture:
                            if (Connection.IsSqlServer())
                            {
                                sqlCall = "( SUBSTRING(" + obj.Value.CommandText + ", 1, " + args[0].Value.ToString().Length + ") = " + args[0].CommandText + ")";
                            }
                            else
                            {
                                sqlCall = "( substr(" + obj.Value.CommandText + ", 1, " + args[0].Value.ToString().Length + ") = " + args[0].CommandText + ")";
                            }
                            break;
                        case StringComparison.OrdinalIgnoreCase:
                        case StringComparison.CurrentCultureIgnoreCase:
                            if (Connection.IsSqlServer())
                            {
                                sqlCall = "(" + obj.Value.CommandText + " like (" + args[0].CommandText + " + N'%'))";
                            }
                            else if (Connection.IsPostgreSql())
                            {
                                sqlCall = "(" + obj.Value.CommandText + " ilike (" + args[0].CommandText + " || '%'))";
                            }
                            else if (Connection.IsMySql())
                            {
                                sqlCall = "(" + obj.Value.CommandText + " like concat(" + args[0].CommandText + ", '%'))";
                            }
                            else
                            {
                                sqlCall = "(" + obj.Value.CommandText + " like (" + args[0].CommandText + " || '%'))";
                            }
                            break;
                    }
                }
                else if (call.Method.Name == nameof(string.EndsWith) && args.Length >= 1)
                {
                    var endsWithCmpOp = StringComparison.CurrentCulture;
                    if (args.Length == 2)
                    {
                        if (args[1].CommandText == GetParameterName("p" + queryArgs.Count.ToString()))
                        {
                            queryArgs.RemoveAt(queryArgs.Count - 1);
                        }
                        endsWithCmpOp = (StringComparison)args[1].Value;
                    }
                    switch (endsWithCmpOp)
                    {
                        case StringComparison.Ordinal:
                        case StringComparison.CurrentCulture:
                            if (Connection.IsSqlServer())
                            {
                                sqlCall = "( SUBSTRING("
                                    + obj.Value.CommandText
                                    + ", LEN("
                                    + obj.Value.CommandText
                                    + ") - "
                                    + args[0].Value.ToString().Length
                                    + "+1, "
                                    + args[0].Value.ToString().Length
                                    + ") = "
                                    + args[0].CommandText
                                    + ")";
                            }
                            else
                            {
                                sqlCall = "( substr("
                                    + obj.Value.CommandText
                                    + ", length("
                                    + obj.Value.CommandText
                                    + ") - "
                                    + args[0].Value.ToString().Length
                                    + "+1, "
                                    + args[0].Value.ToString().Length
                                    + ") = "
                                    + args[0].CommandText
                                    + ")";
                            }
                            break;
                        case StringComparison.OrdinalIgnoreCase:
                        case StringComparison.CurrentCultureIgnoreCase:
                            if (Connection.IsSqlServer())
                            {
                                sqlCall = "(" + obj.Value.CommandText + " like (N'%' + " + args[0].CommandText + "))";
                            }
                            else if (Connection.IsPostgreSql())
                            {
                                sqlCall = "(" + obj.Value.CommandText + " ilike ('%' || " + args[0].CommandText + "))";
                            }
                            else if (Connection.IsMySql())
                            {
                                sqlCall = "(" + obj.Value.CommandText + " like concat('%', " + args[0].CommandText + "))";
                            }
                            else
                            {
                                sqlCall = "(" + obj.Value.CommandText + " like ('%' || " + args[0].CommandText + "))";
                            }
                            break;
                    }
                }
                else if (call.Method.Name == nameof(object.Equals) && args.Length == 1)
                {
                    sqlCall = "(" + obj.Value.CommandText + " = (" + args[0].CommandText + "))";
                }
                else if (call.Method.Name == nameof(string.ToLower))
                {
                    sqlCall = "(lower(" + obj.Value.CommandText + "))";
                }
                else if (call.Method.Name == nameof(string.ToUpper))
                {
                    sqlCall = "(upper(" + obj.Value.CommandText + "))";
                }
                else if (call.Method.Name == nameof(string.Replace) && args.Length == 2)
                {
                    sqlCall = "(replace(" + obj.Value.CommandText + "," + args[0].CommandText + "," + args[1].CommandText + "))";
                }
                else if (call.Method.IsSpecialName && args.Length == 1
                    && (call.Method.Name == "op_Implicit" || call.Method.Name == "op_Explicit"))
                {
                    // .NET 10+ may insert implicit conversion operators (e.g. array to
                    // ReadOnlySpan<T>) in expression trees. Pass through the inner argument.
                    return args[0];
                }
                else
                {
                    sqlCall = call.Method.Name.ToLower() + "(" + string.Join(",", args.Select(a => a.CommandText)) + ")";
                }
                return new CompileResult(
                    commandText: sqlCall);
            }
            else if (expr.NodeType == ExpressionType.Constant)
            {
                var c = (ConstantExpression)expr;
                queryArgs.Add(c.Value);
                return new CompileResult(
                    commandText: GetParameterName("p" + queryArgs.Count.ToString()),
                    value: c.Value);
            }
            else if (expr.NodeType == ExpressionType.Convert)
            {
                var u = (UnaryExpression)expr;
                var ty = u.Type;
                var valr = CompileExpr(u.Operand, queryArgs);
                return new CompileResult(
                    commandText: valr.CommandText,
                    value: valr.Value != null ? ConvertTo(valr.Value, ty) : null);
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
                    return new CompileResult(
                        commandText: EscapeLiteral(columnName));
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
                        if (r.CommandText == GetParameterName("p" + queryArgs.Count.ToString()))
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
                            sb.Append(GetParameterName("p" + queryArgs.Count.ToString()));
                            head = ",";
                        }
                        sb.Append(")");
                        return new CompileResult(
                            commandText: sb.ToString(),
                            value: val);
                    }
                    else
                    {
                        queryArgs.Add(val);
                        return new CompileResult(
                            commandText: GetParameterName("p" + queryArgs.Count.ToString()),
                            value: val);
                    }
                }
            }
            throw new NotSupportedException(
                $"Cannot compile: {expr.NodeType}");
        }

        private static object ConvertTo(object obj, Type t)
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

        private string CompileNullBinaryExpression(BinaryExpression expression, CompileResult parameter)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                    if (parameter.Value == null)
                        return "(" + parameter.CommandText + " is null)";
                    else
                        return "(" + parameter.CommandText + " is $" + parameter.CommandText.TrimStart('[').TrimEnd(']') + ")";
                case ExpressionType.NotEqual:
                    if (parameter.Value == null)
                        return "(" + parameter.CommandText + " is not null)";
                    else
                        return "(" + parameter.CommandText + " is not $" + parameter.CommandText.TrimStart('[').TrimEnd(']') + ")";
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                    return "(" + parameter.CommandText + " < " + parameter.CommandText.TrimStart('[').TrimEnd(']') + ")";
                default:
                    throw new NotSupportedException(
                        $"Cannot compile Null-BinaryExpression with type {expression.NodeType}");
            }
        }

        private static string GetSqlName(Expression expr)
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

        /// <summary>Counts rows matching the query conditions.</summary>
        /// <returns>The number of matching rows.</returns>
        public int Count()
        {
            using (var command = GenerateCommand("count(*)"))
            {
                var result = command.ExecuteScalar();
                if (result is long l)
                {
                    return (int)l;
                }
                return (int)result;
            }
        }

        /// <summary>Counts rows matching the query and the additional predicate.</summary>
        /// <param name="predExpr">The additional filter to combine with the existing query conditions.</param>
        /// <returns>The number of matching rows.</returns>
        public int Count(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).Count();
        }

        /// <summary>Executes the query and returns an enumerator over its materialized rows.</summary>
        /// <returns>An enumerator over the query results.</returns>
        public IEnumerator<T> GetEnumerator()
        {
            return ToList().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>Returns the first row in the query result.</summary>
        /// <returns>The first matching row.</returns>
        /// <exception cref="InvalidOperationException">The query contains no row at the requested position.</exception>
        public T First()
        {
            var query = Take(1);
            return query.ToList().First();
        }

        /// <summary>Returns the first row matching the additional predicate.</summary>
        /// <param name="predExpr">The additional filter to combine with the existing query conditions.</param>
        /// <returns>The first matching row.</returns>
        /// <exception cref="InvalidOperationException">The query contains no row at the requested position.</exception>
        public T First(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).First();
        }

        /// <summary>Returns the first matching row or its default value.</summary>
        /// <returns>The first matching row, or the default value if no row matches.</returns>
        public T FirstOrDefault()
        {
            var query = Take(1);
            return query.ToList().FirstOrDefault();
        }

        /// <summary>Returns the first matching row or its default value.</summary>
        /// <param name="predExpr">The additional filter to combine with the existing query conditions.</param>
        /// <returns>The first matching row, or the default value if no row matches.</returns>
        public T FirstOrDefault(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).FirstOrDefault();
        }
    }

    /// <summary>Builds delegates that assign reader values to mapped properties.</summary>
    static class FastSetter
    {
        /// <summary>Builds mapped property setters when the entity type is only known at runtime.</summary>
        internal static class Typeless
        {
            private static readonly MethodInfo _getSetterMethodInfo;

            static Typeless()
            {
                _getSetterMethodInfo = typeof(FastSetter).GetMethod(
                    nameof(FastSetter.GetFastSetter),
                    BindingFlags.NonPublic | BindingFlags.Static);
            }

            /// <summary>Builds a setter for the specified property using its runtime mapped type.</summary>
            /// <param name="column">The mapped column whose values the setter will assign.</param>
            /// <returns>A setter that accepts the target object, current record, and column ordinal.</returns>
            internal static Action<object, IDataRecord, int> GetFastSetter(
                TableMapping.Column column)
            {
                var getSetter = _getSetterMethodInfo.MakeGenericMethod(column.MappedType);
                return (Action<object, IDataRecord, int>)getSetter.Invoke(null, new object[] { column });
            }
        }

        /// <summary>Builds a delegate that reads a reader field and assigns it to a mapped property.</summary>
        /// <typeparam name="T">The CLR type declaring the mapped property or inheriting it.</typeparam>
        /// <param name="column">The mapped column whose values the setter will assign.</param>
        /// <returns>A setter that accepts the target object, current record, and column ordinal.</returns>
        internal static Action<object, IDataRecord, int> GetFastSetter<T>(
            TableMapping.Column column)
        {
            Action<object, IDataRecord, int> fastSetter = null;
            var clrType = column.PropertyInfo.PropertyType;
            var clrTypeInfo = clrType.GetTypeInfo();

            if (clrTypeInfo.IsGenericType && clrTypeInfo.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                clrType = clrTypeInfo.GenericTypeArguments[0];
                clrTypeInfo = clrType.GetTypeInfo();
            }

            if (clrType == typeof(string))
            {
                fastSetter = CreateTypedSetterDelegate<T, string>(column, (r, i) =>
                {
                    return r.GetString(i);
                });
            }
            else if (clrType == typeof(bool))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, bool>(column, (r, i) =>
                {
                    return r.GetBoolean(i);
                });
            }
            else if (clrType == typeof(int))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, int>(column, (r, i) =>
                {
                    return r.GetInt32(i);
                });
            }
            else if (clrType == typeof(long))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, long>(column, (r, i) =>
                {
                    return r.GetInt64(i);
                });
            }
            else if (clrType == typeof(short))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, short>(column, (r, i) =>
                {
                    return r.GetInt16(i);
                });
            }
            else if (clrType == typeof(decimal))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, decimal>(column, (r, i) =>
                {
                    return r.GetDecimal(i);
                });
            }
            else if (clrType == typeof(double))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, double>(column, (r, i) =>
                {
                    return r.GetDouble(i);
                });
            }
            else if (clrType == typeof(float))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, float>(column, (r, i) =>
                {
                    var value = r.GetValue(i);
                    if (value is double d)
                    {
                        return (float)d;
                    }
                    else
                    {
                        return (float)value;
                    }
                });
            }
            else if (clrType == typeof(DateTime))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, DateTime>(column, (r, i) =>
                {
                    return r.GetDateTime(i);
                });
            }
            else if (clrType == typeof(DateTimeOffset))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, DateTimeOffset>(column, (r, i) =>
                {
                    if (r is DbDataReader dbDataReader)
                    {
                        return dbDataReader.GetFieldValue<DateTimeOffset>(i);
                    }
                    var value = r.GetValue(i);
                    if (value is string s)
                    {
                        return DateTimeOffset.Parse(s);
                    }
                    else
                    {
                        return (DateTimeOffset)value;
                    }
                });
            }
            else if (clrType == typeof(TimeSpan))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, TimeSpan>(column, (r, i) =>
                {
                    var value = r.GetValue(i);
                    if (value is string s)
                    {
                        return TimeSpan.Parse(s);
                    }
                    else if (value != null && value.GetType().FullName == "System.TimeOnly")
                    {
                        var toTimeSpan = value.GetType().GetMethod("ToTimeSpan", Type.EmptyTypes);
                        if (toTimeSpan != null)
                        {
                            return (TimeSpan)toTimeSpan.Invoke(value, null);
                        }
                        return TimeSpan.Parse(value.ToString());
                    }
                    else
                    {
                        return (TimeSpan)value;
                    }
                });
            }
            else if (clrType == typeof(byte[]))
            {
                fastSetter = CreateTypedSetterDelegate<T, byte[]>(column, (r, i) =>
                {
                    return (byte[])r.GetValue(i);
                });
            }
            else if (clrType == typeof(byte))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, byte>(column, (r, i) =>
                {
                    return r.GetByte(i);
                });
            }
            else if (clrType == typeof(sbyte))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, sbyte>(column, (r, i) =>
                {
                    return (sbyte)Convert.ChangeType(r.GetValue(i), typeof(sbyte));
                });
            }
            else if (clrType == typeof(uint))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, uint>(column, (r, i) =>
                {
                    return (uint)Convert.ChangeType(r.GetValue(i), typeof(uint));
                });
            }
            else if (clrType == typeof(ulong))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, ulong>(column, (r, i) =>
                {
                    return (ulong)Convert.ChangeType(r.GetValue(i), typeof(ulong));
                });
            }
            else if (clrType == typeof(ushort))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, ushort>(column, (r, i) =>
                {
                    return (ushort)Convert.ChangeType(r.GetValue(i), typeof(ushort));
                });
            }
            else if (clrType == typeof(Guid))
            {
                fastSetter = CreateNullableTypedSetterDelegate<T, Guid>(column, (r, i) =>
                {
                    return r.GetGuid(i);
                });
            }
            else if (clrType == typeof(Uri))
            {
                fastSetter = CreateTypedSetterDelegate<T, Uri>(column, (r, i) =>
                {
                    var s = r.GetString(i);
                    return new Uri(s);
                });
            }
            else if (clrType == typeof(StringBuilder))
            {
                fastSetter = CreateTypedSetterDelegate<T, StringBuilder>(column, (r, i) =>
                {
                    var s = r.GetString(i);
                    return new StringBuilder(s);
                });
            }
            else if (clrType == typeof(UriBuilder))
            {
                fastSetter = CreateTypedSetterDelegate<T, UriBuilder>(column, (r, i) =>
                {
                    var s = r.GetString(i);
                    return new UriBuilder(s);
                });
            }

            return fastSetter;
        }

        private static Action<object, IDataRecord, int> CreateNullableTypedSetterDelegate<TObject, TColumnMember>(
            TableMapping.Column column,
            Func<IDataRecord, int, TColumnMember> getColumnValue)
            where TColumnMember : struct
        {
            var clrTypeInfo = column.PropertyInfo.PropertyType.GetTypeInfo();
            var isNullable = clrTypeInfo.IsGenericType && clrTypeInfo.GetGenericTypeDefinition() == typeof(Nullable<>);
            if (isNullable)
            {
                var setProperty = (Action<TObject, TColumnMember?>)Delegate.CreateDelegate(
                    typeof(Action<TObject, TColumnMember?>),
                    null,
                    column.PropertyInfo.GetSetMethod());
                return (o, r, i) =>
                {
                    if (!r.IsDBNull(i))
                    {
                        setProperty.Invoke((TObject)o, getColumnValue(r, i));
                    }
                };
            }
            return CreateTypedSetterDelegate<TObject, TColumnMember>(column, getColumnValue);
        }

        private static Action<object, IDataRecord, int> CreateTypedSetterDelegate<TObject, TColumnMember>(
            TableMapping.Column column,
            Func<IDataRecord, int, TColumnMember> getColumnValue)
        {
            var setProperty = (Action<TObject, TColumnMember>)Delegate.CreateDelegate(
                typeof(Action<TObject, TColumnMember>),
                null,
                column.PropertyInfo.GetSetMethod());
            return (o, r, i) =>
            {
                if (!r.IsDBNull(i))
                {
                    setProperty.Invoke((TObject)o, getColumnValue(r, i));
                }
            };
        }
    }
}
