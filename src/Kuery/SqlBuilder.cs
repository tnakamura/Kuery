using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Kuery
{
    /// <summary>Builds parameterized ADO.NET commands for mapped operations and raw SQL.</summary>
    internal static class SqlBuilder
    {
        /// <summary>Creates a select command for a mapped entity lookup.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <param name="map">The mapping that associates CLR properties with database columns.</param>
        /// <param name="pk">The primary key value to bind for the row lookup.</param>
        /// <returns>The command with its key argument bound when a key is mapped.</returns>
        internal static IDbCommand CreateGetByPrimaryKeyCommand(this IDbConnection connection, TableMapping map, object pk)
        {
            var command = connection.CreateCommand();

            string commandText;
            if (map.PK != null)
            {
                commandText = $"select * from " +
                    connection.EscapeLiteral(map.TableName) +
                    $" where " +
                    connection.EscapeLiteral(map.PK.Name) +
                    $" = " +
                    connection.GetParameterName(map.PK.Name);
            }
            else
            {
                commandText = "select top 1 * from " +
                    connection.EscapeLiteral(map.TableName);
            }

            command.CommandText = commandText;
            if (map.PK != null)
            {
                var pkParameter = command.CreateParameter();
                pkParameter.ParameterName = connection.GetParameterName(map.PK.Name);
                pkParameter.Value = pk;
                command.Parameters.Add(pkParameter);
            }
            return command;
        }

        /// <summary>Creates a provider-specific command that reads the last generated numeric key.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <returns>The command that reads the generated key.</returns>
        internal static IDbCommand CreateLastInsertRowIdCommand(this IDbConnection connection)
        {
            string commandText;
            if (connection.IsSqlite())
            {
                commandText = "select last_insert_rowid();";
            }
            else if (connection.IsPostgreSql())
            {
                commandText = "SELECT LASTVAL();";
            }
            else if (connection.IsMySql())
            {
                commandText = "SELECT LAST_INSERT_ID();";
            }
            else
            {
                commandText = "select @@IDENTITY";
            }
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            return command;
        }

        /// <summary>Creates an insert command using the mapped insert columns and entity values.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <param name="item">The entity supplying mapped values for the command.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <returns>The parameterized insert command.</returns>
        internal static IDbCommand CreateInsertCommand(this IDbConnection connection, object item, Type type)
        {
            var map = SqlMapper.GetMapping(type);
            var columns = new StringBuilder();
            var values = new StringBuilder();
            var command = connection.CreateCommand();

            if (map.InsertColumns.Count == 0 && map.Columns.Count > 0 && map.HasAutoIncPK)
            {
                command.CommandText = "insert into "
                    + connection.EscapeLiteral(map.TableName)
                    + " default values";
            }
            else
            {
                for (var i = 0; i < map.InsertColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        columns.Append(",");
                        values.Append(",");
                    }

                    var col = map.InsertColumns[i];
                    columns.Append(connection.EscapeLiteral(col.Name));

                    var value = col.GetValue(item);
                    if (value is null && col.IsNullable)
                    {
                        values.Append("NULL");
                    }
                    else
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = connection.GetParameterName(col.Name);
                        if (col.ColumnType.IsEnum)
                        {
                            if (col.StoreAsText)
                            {
                                parameter.Value = value.ToString();
                            }
                            else
                            {
                                var underlyingType = col.ColumnType.GetEnumUnderlyingType();
                                parameter.Value = Convert.ChangeType(value, underlyingType);
                            }
                        }
                        else
                        {
                            parameter.Value = value;
                        }
                        command.Parameters.Add(parameter);
                        values.Append(parameter.ParameterName);
                    }
                }

                command.CommandText = "insert into "
                    + connection.EscapeLiteral(map.TableName)
                    + " ("
                    + columns.ToString()
                    + ") values ("
                    + values.ToString()
                    + ");";
            }

            return command;
        }

        /// <summary>Creates an update command using the entity values and mapped key.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <param name="item">The entity supplying mapped values for the command.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <returns>The parameterized update command.</returns>
        internal static IDbCommand CreateUpdateCommand(this IDbConnection connection, object item, Type type)
        {
            var mapping = SqlMapper.GetMapping(type);
            if (mapping.PK == null)
            {
                throw new NotSupportedException(
                    $"Cannot update {mapping.TableName}: it has no PK");
            }

            var sql = new StringBuilder();
            sql.Append("update ");
            sql.Append(connection.EscapeLiteral(mapping.TableName));
            sql.Append(" ");

            var command = connection.CreateCommand();

            var cols = mapping.Columns.Where(x => x != mapping.PK).ToArray();
            if (cols.Length == 0)
            {
                cols = mapping.Columns.ToArray();
            }

            for (var i = 0; i < cols.Length; i++)
            {
                var col = cols[i];
                var parameter = command.CreateParameter();
                parameter.ParameterName = connection.GetParameterName(col.Name);
                parameter.Value = col.GetValue(item);
                command.Parameters.Add(parameter);

                if (i == 0)
                {
                    sql.Append(" set ");
                }
                else
                {
                    sql.Append(",");
                }
                sql.Append(connection.EscapeLiteral(col.Name));
                sql.Append(" = ");
                sql.Append(parameter.ParameterName);
            }

            sql.Append(" where ");
            sql.Append(connection.EscapeLiteral(mapping.PK.Name));
            sql.Append(" = ");
            sql.Append(connection.GetParameterName(mapping.PK.Name));

            var pkParameterName = connection.GetParameterName(mapping.PK.Name);
            if (!command.Parameters.Contains(pkParameterName))
            {
                var pkParamter = command.CreateParameter();
                pkParamter.ParameterName = pkParameterName;
                pkParamter.Value = mapping.PK.GetValue(item);
                command.Parameters.Add(pkParamter);
            }

            command.CommandText = sql.ToString();
            return command;
        }

        /// <summary>Wraps a mapped name in the provider's identifier delimiters.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <param name="name">The database table or column name to delimit.</param>
        /// <returns>The delimited identifier.</returns>
        internal static string EscapeLiteral(this IDbConnection connection, string name)
        {
            if (connection.IsPostgreSql())
            {
                return "\"" + name + "\"";
            }
            if (connection.IsMySql())
            {
                return "`" + name + "`";
            }
            return "[" + name + "]";
        }

        /// <summary>Checks the concrete connection type against the known SQLite provider types.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <returns>True when the connection type is recognized as SQLite.</returns>
        internal static bool IsSqlite(this IDbConnection connection)
        {
            switch (connection.GetType().FullName)
            {
                case "System.Data.Sqlite.SqliteConnection":
                case "Microsoft.Data.Sqlite.SqliteConnection":
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>Checks whether the connection type is the Npgsql provider type.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <returns>True when the connection type is recognized as PostgreSQL.</returns>
        internal static bool IsPostgreSql(this IDbConnection connection)
        {
            switch (connection.GetType().FullName)
            {
                case "Npgsql.NpgsqlConnection":
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>Checks the concrete connection type against the known SqlClient provider types.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <returns>True when the connection type is recognized as SQL Server.</returns>
        internal static bool IsSqlServer(this IDbConnection connection)
        {
            switch (connection.GetType().FullName)
            {
                case "System.Data.SqlClient.SqlConnection":
                case "Microsoft.Data.SqlClient.SqlConnection":
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>Checks whether the connection type is the MySqlConnector provider type.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <returns>True when the connection type is recognized as MySQL.</returns>
        internal static bool IsMySql(this IDbConnection connection)
        {
            switch (connection.GetType().FullName)
            {
                case "MySqlConnector.MySqlConnection":
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>Selects the parameter prefix for the connection provider.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <returns>The prefix used in SQL command text and parameter names.</returns>
        internal static string GetParameterPrefix(this IDbConnection connection)
        {
            if (connection.IsSqlite())
                return "$";
            else if (connection.IsSqlServer())
                return "@";
            else if (connection.IsPostgreSql())
                return "@";
            else if (connection.IsMySql())
                return "@";
            else
                return "@";
        }

        /// <summary>Adds the provider prefix to an unprefixed parameter name.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <param name="name">The parameter name before adding its provider prefix.</param>
        /// <returns>The complete parameter name used by the command.</returns>
        internal static string GetParameterName(this IDbConnection connection, string name)
        {
            return connection.GetParameterPrefix() + name;
        }

        /// <summary>Creates a delete command targeting a mapped primary key.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <param name="item">The entity whose mapped primary key identifies the row to delete.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <returns>The parameterized delete command.</returns>
        internal static IDbCommand CreateDeleteCommand(this IDbConnection connection, object item, Type type)
        {
            var map = SqlMapper.GetMapping(type);
            if (map.PK == null)
            {
                throw new NotSupportedException(
                    $"Cannot update {map.TableName}: it has no PK");
            }

            return connection.CreateDeleteCommand(map.PK.GetValue(item), map);
        }

        /// <summary>Creates a delete command targeting a mapped primary key.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <param name="primaryKey">The mapped primary key value of the row to delete.</param>
        /// <param name="map">The mapping that associates CLR properties with database columns.</param>
        /// <returns>The parameterized delete command.</returns>
        internal static IDbCommand CreateDeleteCommand(this IDbConnection connection, object primaryKey, TableMapping map)
        {
            var pk = map.PK;
            if (pk == null)
            {
                throw new NotSupportedException(
                    $"Cannot delete {map.TableName}: it has no PK");
            }
            var pkParamName = connection.GetParameterName("pk");

            var query = $"DELETE FROM {connection.EscapeLiteral(map.TableName)} where {connection.EscapeLiteral(pk.Name)} = {pkParamName}";

            var command = connection.CreateCommand();
            command.CommandText = query;

            var pkParameter = command.CreateParameter();
            pkParameter.ParameterName = pkParamName;
            pkParameter.Value = primaryKey;
            command.Parameters.Add(pkParameter);

            return command;
        }

        /// <summary>Creates a provider-specific write command that handles row conflicts.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <param name="item">The entity supplying mapped values for the command.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <returns>The parameterized command for inserting or replacing the row.</returns>
        internal static IDbCommand CreateInsertOrReplaceCommand(this IDbConnection connection, object item, Type type)
        {
            if (connection.IsSqlite())
            {
                return connection.CreateInsertOrReplaceCommandForSqlite(item, type);
            }
            else if (connection.IsPostgreSql())
            {
                return connection.CreateInsertOrReplaceCommandForPostgreSql(item, type);
            }
            else if (connection.IsMySql())
            {
                return connection.CreateInsertOrReplaceCommandForMySql(item, type);
            }
            else
            {
                return connection.CreateMergeCommandForSqlServer(item, type);
            }
        }

        private static IDbCommand CreateMergeCommandForSqlServer(this IDbConnection connection, object item, Type type)
        {
            var map = SqlMapper.GetMapping(type);
            var command = connection.CreateCommand();

            if (map.InsertOrReplaceColumns.Count == 0 && map.Columns.Count > 0 && map.HasAutoIncPK)
            {
                command.CommandText = $@"MERGE [{map.TableName}] AS tgt
USING (SELECT @{map.PK.Name}) AS src ([{map.PK.Name}])
ON (tgt.[{map.PK.Name}] = src.[{map.PK.Name}])
WHEN NOT MATCHED THEN
    INSERT [{map.TableName}] DEFAULT VALUES;
";
                var parameter = command.CreateParameter();
                parameter.ParameterName = "@" + map.PK.Name;
                parameter.Value = map.PK.GetValue(item);
                command.Parameters.Add(parameter);
            }
            else
            {
                if (map.PK.IsAutoInc)
                {
                    var pkParameter = command.CreateParameter();
                    pkParameter.ParameterName = "@" + map.PK.Name;
                    pkParameter.Value = map.PK.GetValue(item);
                    command.Parameters.Add(pkParameter);
                }

                var insertColumns = new StringBuilder();
                var updateColumns = new StringBuilder();
                var values = new StringBuilder();
                for (var i = 0; i < map.InsertColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        insertColumns.Append(",");
                        updateColumns.Append(",");
                        values.Append(",");
                    }

                    var col = map.InsertColumns[i];
                    insertColumns.Append("[");
                    insertColumns.Append(col.Name);
                    insertColumns.Append("]");
                    updateColumns.Append("[");
                    updateColumns.Append(col.Name);
                    updateColumns.Append("] = ");

                    var value = col.GetValue(item);
                    if (value is null && col.IsNullable)
                    {
                        values.Append("NULL");
                    }
                    else
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = connection.GetParameterName(col.Name);
                        parameter.Value = col.GetValue(item);
                        command.Parameters.Add(parameter);

                        values.Append(parameter.ParameterName);
                        updateColumns.Append(parameter.ParameterName);
                    }
                }

                command.CommandText = $@"MERGE [{map.TableName}] AS tgt
USING (SELECT @{map.PK.Name}) AS src ([{map.PK.Name}])
ON (tgt.[{map.PK.Name}] = src.[{map.PK.Name}])
WHEN MATCHED THEN
    UPDATE SET {updateColumns}
WHEN NOT MATCHED THEN
    INSERT ({insertColumns})
    VALUES ({values});";
            }

            return command;
        }

        private static IDbCommand CreateInsertOrReplaceCommandForPostgreSql(this IDbConnection connection, object item, Type type)
        {
            var map = SqlMapper.GetMapping(type);
            var command = connection.CreateCommand();

            if (map.InsertOrReplaceColumns.Count == 0 && map.Columns.Count > 0 && map.HasAutoIncPK)
            {
                command.CommandText = "insert into "
                    + "\"" + map.TableName + "\""
                    + " default values"
                    + " on conflict(\"" + map.PK.Name + "\")"
                    + " do nothing;";
            }
            else
            {
                var insertColumns = new StringBuilder();
                var updateColumns = new StringBuilder();
                var values = new StringBuilder();
                for (var i = 0; i < map.InsertOrReplaceColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        insertColumns.Append(",");
                        updateColumns.Append(",");
                        values.Append(",");
                    }

                    var col = map.InsertOrReplaceColumns[i];
                    insertColumns.Append("\"");
                    insertColumns.Append(col.Name);
                    insertColumns.Append("\"");
                    updateColumns.Append("\"");
                    updateColumns.Append(col.Name);
                    updateColumns.Append("\" = ");

                    var value = col.GetValue(item);
                    if (value is null && col.IsNullable)
                    {
                        values.Append("NULL");
                    }
                    else
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = connection.GetParameterName(col.Name);
                        parameter.Value = col.GetValue(item);
                        command.Parameters.Add(parameter);

                        values.Append(parameter.ParameterName);
                        updateColumns.Append(parameter.ParameterName);
                    }
                }

                command.CommandText = "insert into "
                    + "\"" + map.TableName + "\""
                    + " ("
                    + insertColumns.ToString()
                    + ") values ("
                    + values.ToString()
                    + ")"
                    + " on conflict(\"" + map.PK.Name + "\")"
                    + " do update set "
                    + updateColumns.ToString()
                    + ";";
            }

            return command;
        }

        private static IDbCommand CreateInsertOrReplaceCommandForSqlite(this IDbConnection connection, object item, Type type)
        {
            var map = SqlMapper.GetMapping(type);
            var command = connection.CreateCommand();

            if (map.InsertOrReplaceColumns.Count == 0 && map.Columns.Count > 0 && map.HasAutoIncPK)
            {
                command.CommandText = "insert or replace into ["
                    + map.TableName
                    + "] default values";
            }
            else
            {
                var columns = new StringBuilder();
                var values = new StringBuilder();
                for (var i = 0; i < map.InsertOrReplaceColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        columns.Append(",");
                        values.Append(",");
                    }

                    var col = map.InsertOrReplaceColumns[i];
                    columns.Append("[");
                    columns.Append(col.Name);
                    columns.Append("]");

                    var value = col.GetValue(item);
                    if (value is null && col.IsNullable)
                    {
                        values.Append("NULL");
                    }
                    else
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = connection.GetParameterName(col.Name);
                        parameter.Value = col.GetValue(item);
                        command.Parameters.Add(parameter);
                        values.Append(parameter.ParameterName);
                    }
                }

                command.CommandText = "insert or replace into ["
                    + map.TableName
                    + "] ("
                    + columns.ToString()
                    + ") values ("
                    + values.ToString()
                    + ");";
            }

            return command;
        }

        private static IDbCommand CreateInsertOrReplaceCommandForMySql(this IDbConnection connection, object item, Type type)
        {
            var map = SqlMapper.GetMapping(type);
            var command = connection.CreateCommand();

            if (map.InsertOrReplaceColumns.Count == 0 && map.Columns.Count > 0 && map.HasAutoIncPK)
            {
                command.CommandText = "insert into "
                    + "`" + map.TableName + "`"
                    + " () values ()"
                    + " on duplicate key update "
                    + "`" + map.PK.Name + "` = " + "`" + map.PK.Name + "`;";
            }
            else
            {
                var insertColumns = new StringBuilder();
                var updateColumns = new StringBuilder();
                var values = new StringBuilder();
                for (var i = 0; i < map.InsertOrReplaceColumns.Count; i++)
                {
                    if (i > 0)
                    {
                        insertColumns.Append(",");
                        updateColumns.Append(",");
                        values.Append(",");
                    }

                    var col = map.InsertOrReplaceColumns[i];
                    insertColumns.Append("`");
                    insertColumns.Append(col.Name);
                    insertColumns.Append("`");
                    updateColumns.Append("`");
                    updateColumns.Append(col.Name);
                    updateColumns.Append("` = ");

                    var value = col.GetValue(item);
                    if (value is null && col.IsNullable)
                    {
                        values.Append("NULL");
                    }
                    else
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = connection.GetParameterName(col.Name);
                        parameter.Value = col.GetValue(item);
                        command.Parameters.Add(parameter);

                        values.Append(parameter.ParameterName);
                        updateColumns.Append(parameter.ParameterName);
                    }
                }

                command.CommandText = "insert into "
                    + "`" + map.TableName + "`"
                    + " ("
                    + insertColumns.ToString()
                    + ") values ("
                    + values.ToString()
                    + ")"
                    + " on duplicate key update "
                    + updateColumns.ToString()
                    + ";";
            }

            return command;
        }

        /// <summary>Creates a command with parameter values supplied by the caller.</summary>
        /// <param name="connection">The connection associated with the query or command.</param>
        /// <param name="sql">The SQL command text.</param>
        /// <param name="param">The optional parameter values supplied as a dictionary or an object with public fields or properties.</param>
        /// <returns>The command with its SQL text and parameters assigned.</returns>
        /// <remarks>The caller owns the returned command.</remarks>
        internal static IDbCommand CreateParameterizedCommand(this IDbConnection connection, string sql, object param = null)
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
                        parameter.ParameterName = connection.GetParameterName(kvp.Key);
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
                        parameter.ParameterName = connection.GetParameterName(property.Name);
                        parameter.Value = property.GetValue(param);
                        command.Parameters.Add(parameter);
                    }
                    var fields = paramType.GetFields(BindingFlags.Public | BindingFlags.Instance);
                    foreach (var field in fields)
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = connection.GetParameterName(field.Name);
                        parameter.Value = field.GetValue(param);
                        command.Parameters.Add(parameter);
                    }
                }
            }

            return command;
        }
    }
}
