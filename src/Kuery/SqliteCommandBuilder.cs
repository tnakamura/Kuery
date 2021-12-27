using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Kuery
{
    internal static class SqliteCommandBuilder
    {
        internal static DbCommand CreateGetByPrimaryKeyCommand(this DbConnection connection, TableMapping map, object pk)
        {
            var command = connection.CreateCommand();
            command.CommandText = map.GetByPrimaryKeySql;
            if (map.PK != null)
            {
                var pkParameter = command.CreateParameter();
                pkParameter.ParameterName = connection.GetParameterName(map.PK.Name);
                pkParameter.Value = pk;
                command.Parameters.Add(pkParameter);
            }
            return command;
        }

        internal static DbCommand CreateLastInsertRowIdCommand(this DbConnection connection)
        {
            var command = connection.CreateCommand();
            command.CommandText = "select last_insert_rowid();";
            return command;
        }

        internal static DbCommand CreateInsertCommand(this DbConnection connection, object item, Type type)
        {
            var map = SqlHelper.GetMapping(type);
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
                    parameter.ParameterName = connection.GetParameterName(col.Name);
                    parameter.Value = col.GetValue(item);
                    command.Parameters.Add(parameter);
                    values.Append(parameter.ParameterName);
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

        internal static DbCommand CreateUpdateCommand(this DbConnection connection, object item, Type type)
        {
            var mapping = SqlHelper.GetMapping(type);
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
                parameter.ParameterName = connection.GetParameterName(col.Name);
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
            sql.Append(" = ");
            sql.Append(connection.GetParameterName(mapping.PK.Name));

            var pkParamter = command.CreateParameter();
            pkParamter.ParameterName = connection.GetParameterName(mapping.PK.Name);
            pkParamter.Value = mapping.PK.GetValue(item);
            command.Parameters.Add(pkParamter);

            command.CommandText = sql.ToString();
            return command;
        }

        internal static string GetParameterPrefix(this DbConnection connection)
        {
            switch (connection.GetType().FullName)
            {
                case "System.Data.Sqlite.SqliteConnection":
                case "Microsoft.Data.Sqlite.SqliteConnection":
                    return "$";
                default:
                    return "@";
            }
        }

        internal static string GetParameterName(this DbConnection connection, string name)
        {
            return connection.GetParameterPrefix() + name;
        }

        internal static DbCommand CreateDeleteCommand(this DbConnection connection, object item, Type type)
        {
            var map = SqlHelper.GetMapping(type);
            if (map.PK == null)
            {
                throw new NotSupportedException(
                    $"Cannot update {map.TableName}: it has no PK");
            }

            return connection.CreateDeleteCommand(map.PK.GetValue(item), map);
        }

        internal static DbCommand CreateDeleteCommand(this DbConnection connection, object primaryKey, TableMapping map)
        {
            var pk = map.PK;
            if (pk == null)
            {
                throw new NotSupportedException(
                    $"Cannot delete {map.TableName}: it has no PK");
            }
            var pkParamName = connection.GetParameterName("pk");

            var query = $"DELETE FROM [{map.TableName}] where [{pk.Name}] = {pkParamName}";

            var command = connection.CreateCommand();
            command.CommandText = query;

            var pkParameter = command.CreateParameter();
            pkParameter.ParameterName = pkParamName;
            pkParameter.Value = primaryKey;
            command.Parameters.Add(pkParameter);

            return command;
        }

        internal static DbCommand CreateMergeCommand(this DbConnection connection, object item, Type type)
        {
            var map = SqlHelper.GetMapping(type);
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
                p.ParameterName = connection.GetParameterName(c.Name);
                p.Value = c.GetValue(item);
                command.Parameters.Add(p);
            }

            return command;
        }

        internal static DbCommand CreateParameterizedCommand(this DbConnection connection, string sql, object param = null)
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
