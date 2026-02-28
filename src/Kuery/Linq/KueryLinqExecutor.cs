using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kuery.Linq
{
    internal sealed class KueryLinqExecutor : IKueryLinqExecutor
    {
        readonly QueryableModelTranslator _translator = new QueryableModelTranslator();

        readonly SelectSqlGenerator _sqlGenerator = new SelectSqlGenerator();

        public object BuildQueryModel(KueryQueryContext context, Expression expression)
        {
            Requires.NotNull(context, nameof(context));
            Requires.NotNull(expression, nameof(expression));
            return _translator.Translate(expression);
        }

        public object Execute(KueryQueryContext context, Expression expression, Type resultType)
        {
            Requires.NotNull(context, nameof(context));
            Requires.NotNull(expression, nameof(expression));
            Requires.NotNull(resultType, nameof(resultType));

            var model = (SelectQueryModel)BuildQueryModel(context, expression);
            var dialect = SqlDialectFactory.Create(context.Connection);
            var generated = _sqlGenerator.Generate(model, dialect);
            return ExecuteCore(context.Connection, model, generated, resultType);
        }

        public async Task<object> ExecuteAsync(KueryQueryContext context, Expression expression, Type resultType, CancellationToken cancellationToken)
        {
            Requires.NotNull(context, nameof(context));
            Requires.NotNull(expression, nameof(expression));
            Requires.NotNull(resultType, nameof(resultType));

            var model = (SelectQueryModel)BuildQueryModel(context, expression);
            var dialect = SqlDialectFactory.Create(context.Connection);
            var generated = _sqlGenerator.Generate(model, dialect);
            return await ExecuteCoreAsync(context.Connection, model, generated, resultType, cancellationToken).ConfigureAwait(false);
        }

        public object ExecuteTerminal(KueryQueryContext context, Expression expression)
        {
            return Execute(context, expression, expression.Type);
        }

        public Task<object> ExecuteTerminalAsync(KueryQueryContext context, Expression expression, CancellationToken cancellationToken)
        {
            return ExecuteAsync(context, expression, expression.Type, cancellationToken);
        }

        private static object ExecuteCore(IDbConnection connection, SelectQueryModel model, GeneratedSql generated, Type resultType)
        {
            if (model.Join != null)
            {
                return ExecuteJoinCore(connection, model, generated, resultType);
            }

            if (model.GroupBySelectItems != null && model.GroupBySelectItems.Count > 0)
            {
                return ExecuteGroupByCore(connection, model, generated, resultType);
            }

            using (var command = CreateCommand(connection, generated))
            {
                switch (model.Terminal)
                {
                    case QueryTerminalKind.Count:
                        return ExecuteCount(command);
                    case QueryTerminalKind.LongCount:
                        return ExecuteLongCount(command);
                    case QueryTerminalKind.Any:
                        return ExecuteCount(command) > 0;
                    case QueryTerminalKind.All:
                        return ExecuteCount(command) == 0;
                    case QueryTerminalKind.Sum:
                    case QueryTerminalKind.Min:
                    case QueryTerminalKind.Max:
                    case QueryTerminalKind.Average:
                        return ExecuteAggregate(command, resultType);
                    case QueryTerminalKind.First:
                        return ExecuteFirst(command, model.Table, model.Projection, throwIfEmpty: true);
                    case QueryTerminalKind.FirstOrDefault:
                        return ExecuteFirst(command, model.Table, model.Projection, throwIfEmpty: false);
                    case QueryTerminalKind.Last:
                        return ExecuteFirst(command, model.Table, model.Projection, throwIfEmpty: true);
                    case QueryTerminalKind.LastOrDefault:
                        return ExecuteFirst(command, model.Table, model.Projection, throwIfEmpty: false);
                    case QueryTerminalKind.ElementAt:
                        return ExecuteFirst(command, model.Table, model.Projection, throwIfEmpty: true);
                    case QueryTerminalKind.ElementAtOrDefault:
                        return ExecuteFirst(command, model.Table, model.Projection, throwIfEmpty: false);
                    case QueryTerminalKind.Single:
                        return ExecuteSingle(command, model.Table, model.Projection, throwIfEmpty: true);
                    case QueryTerminalKind.SingleOrDefault:
                        return ExecuteSingle(command, model.Table, model.Projection, throwIfEmpty: false);
                    case QueryTerminalKind.Sequence:
                    default:
                        return ExecuteSequence(command, model.Table, model.Projection, resultType);
                }
            }
        }

        private static async Task<object> ExecuteCoreAsync(
            IDbConnection connection,
            SelectQueryModel model,
            GeneratedSql generated,
            Type resultType,
            CancellationToken cancellationToken)
        {
            if (model.Join != null)
            {
                return await ExecuteJoinCoreAsync(connection, model, generated, resultType, cancellationToken).ConfigureAwait(false);
            }

            if (model.GroupBySelectItems != null && model.GroupBySelectItems.Count > 0)
            {
                return await ExecuteGroupByCoreAsync(connection, model, generated, resultType, cancellationToken).ConfigureAwait(false);
            }

            using (var command = CreateCommand(connection, generated))
            {
                switch (model.Terminal)
                {
                    case QueryTerminalKind.Count:
                        return await ExecuteCountAsync(command, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.LongCount:
                        return await ExecuteLongCountAsync(command, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.Any:
                        return await ExecuteCountAsync(command, cancellationToken).ConfigureAwait(false) > 0;
                    case QueryTerminalKind.All:
                        return await ExecuteCountAsync(command, cancellationToken).ConfigureAwait(false) == 0;
                    case QueryTerminalKind.Sum:
                    case QueryTerminalKind.Min:
                    case QueryTerminalKind.Max:
                    case QueryTerminalKind.Average:
                        return await ExecuteAggregateAsync(command, resultType, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.First:
                        return await ExecuteFirstAsync(command, model.Table, model.Projection, throwIfEmpty: true, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.FirstOrDefault:
                        return await ExecuteFirstAsync(command, model.Table, model.Projection, throwIfEmpty: false, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.Last:
                        return await ExecuteFirstAsync(command, model.Table, model.Projection, throwIfEmpty: true, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.LastOrDefault:
                        return await ExecuteFirstAsync(command, model.Table, model.Projection, throwIfEmpty: false, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.ElementAt:
                        return await ExecuteFirstAsync(command, model.Table, model.Projection, throwIfEmpty: true, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.ElementAtOrDefault:
                        return await ExecuteFirstAsync(command, model.Table, model.Projection, throwIfEmpty: false, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.Single:
                        return await ExecuteSingleAsync(command, model.Table, model.Projection, throwIfEmpty: true, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.SingleOrDefault:
                        return await ExecuteSingleAsync(command, model.Table, model.Projection, throwIfEmpty: false, cancellationToken).ConfigureAwait(false);
                    case QueryTerminalKind.Sequence:
                    default:
                        return await ExecuteSequenceAsync(command, model.Table, model.Projection, resultType, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private static IDbCommand CreateCommand(IDbConnection connection, GeneratedSql generated)
        {
            var command = connection.CreateCommand();
            command.CommandText = generated.CommandText;
            for (var i = 0; i < generated.Parameters.Count; i++)
            {
                var parameter = command.CreateParameter();
                parameter.ParameterName = connection.GetParameterName("p" + (i + 1).ToString());
                parameter.Value = generated.Parameters[i] ?? DBNull.Value;
                command.Parameters.Add(parameter);
            }

            return command;
        }

        private static object ExecuteJoinCore(IDbConnection connection, SelectQueryModel model, GeneratedSql generated, Type resultType)
        {
            using (var command = CreateCommand(connection, generated))
            {
                switch (model.Terminal)
                {
                    case QueryTerminalKind.Count:
                    case QueryTerminalKind.LongCount:
                    case QueryTerminalKind.Any:
                    case QueryTerminalKind.All:
                        var countResult = model.Terminal == QueryTerminalKind.LongCount
                            ? (object)ExecuteLongCount(command)
                            : ExecuteCount(command);
                        if (model.Terminal == QueryTerminalKind.Any)
                            return (int)countResult > 0;
                        if (model.Terminal == QueryTerminalKind.All)
                            return (int)countResult == 0;
                        return countResult;
                    default:
                        return ExecuteJoinSequence(command, model, resultType);
                }
            }
        }

        private static async Task<object> ExecuteJoinCoreAsync(
            IDbConnection connection,
            SelectQueryModel model,
            GeneratedSql generated,
            Type resultType,
            CancellationToken cancellationToken)
        {
            using (var command = CreateCommand(connection, generated))
            {
                switch (model.Terminal)
                {
                    case QueryTerminalKind.Count:
                    case QueryTerminalKind.LongCount:
                    case QueryTerminalKind.Any:
                    case QueryTerminalKind.All:
                        var countResult = model.Terminal == QueryTerminalKind.LongCount
                            ? (object)await ExecuteLongCountAsync(command, cancellationToken).ConfigureAwait(false)
                            : await ExecuteCountAsync(command, cancellationToken).ConfigureAwait(false);
                        if (model.Terminal == QueryTerminalKind.Any)
                            return (int)countResult > 0;
                        if (model.Terminal == QueryTerminalKind.All)
                            return (int)countResult == 0;
                        return countResult;
                    default:
                        return await ExecuteJoinSequenceAsync(command, model, resultType, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private static object ExecuteJoinSequence(IDbCommand command, SelectQueryModel model, Type resultType)
        {
            var outerTable = model.Table;
            var innerTable = model.Join.InnerTable;
            var resultSelector = model.Join.ResultSelector.Compile();
            var outerColCount = outerTable.Columns.Count;

            var results = new List<object>();
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var outer = ReadObject(reader, outerTable, 0);
                    var inner = ReadObject(reader, innerTable, outerColCount);
                    var result = resultSelector.DynamicInvoke(outer, inner);
                    results.Add(result);
                }
            }

            return HandleJoinTerminal(results, model.Terminal, resultType);
        }

        private static async Task<object> ExecuteJoinSequenceAsync(IDbCommand command, SelectQueryModel model, Type resultType, CancellationToken cancellationToken)
        {
            var outerTable = model.Table;
            var innerTable = model.Join.InnerTable;
            var resultSelector = model.Join.ResultSelector.Compile();
            var outerColCount = outerTable.Columns.Count;

            var results = new List<object>();
            if (command is System.Data.Common.DbCommand dbCommand)
            {
                using (var reader = await dbCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var outer = ReadObject(reader, outerTable, 0);
                        var inner = ReadObject(reader, innerTable, outerColCount);
                        var result = resultSelector.DynamicInvoke(outer, inner);
                        results.Add(result);
                    }
                }
            }
            else
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var outer = ReadObject(reader, outerTable, 0);
                        var inner = ReadObject(reader, innerTable, outerColCount);
                        var result = resultSelector.DynamicInvoke(outer, inner);
                        results.Add(result);
                    }
                }
            }

            return HandleJoinTerminal(results, model.Terminal, resultType);
        }

        private static object ExecuteGroupByCore(IDbConnection connection, SelectQueryModel model, GeneratedSql generated, Type resultType)
        {
            using (var command = CreateCommand(connection, generated))
            {
                var results = ReadGroupByResults(command, model);
                return HandleGroupByTerminal(results, model.Terminal, resultType);
            }
        }

        private static async Task<object> ExecuteGroupByCoreAsync(
            IDbConnection connection,
            SelectQueryModel model,
            GeneratedSql generated,
            Type resultType,
            CancellationToken cancellationToken)
        {
            using (var command = CreateCommand(connection, generated))
            {
                List<object> results;
                if (command is System.Data.Common.DbCommand dbCommand)
                {
                    results = new List<object>();
                    using (var reader = await dbCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            results.Add(ReadGroupByRow(reader, model));
                        }
                    }
                }
                else
                {
                    results = ReadGroupByResults(command, model);
                }

                return HandleGroupByTerminal(results, model.Terminal, resultType);
            }
        }

        private static List<object> ReadGroupByResults(IDbCommand command, SelectQueryModel model)
        {
            var results = new List<object>();
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    results.Add(ReadGroupByRow(reader, model));
                }
            }
            return results;
        }

        private static object ReadGroupByRow(IDataReader reader, SelectQueryModel model)
        {
            var constructor = model.GroupByResultConstructor;
            var selectItems = model.GroupBySelectItems;
            var parameters = constructor.GetParameters();
            var args = new object[selectItems.Count];

            for (var i = 0; i < selectItems.Count; i++)
            {
                var val = reader.GetValue(i);
                if (val is DBNull)
                {
                    val = null;
                }

                var targetType = parameters[i].ParameterType;
                var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

                if (val != null)
                {
                    args[i] = Convert.ChangeType(val, underlying);
                }
                else if (targetType.IsValueType && Nullable.GetUnderlyingType(targetType) == null)
                {
                    args[i] = Activator.CreateInstance(targetType);
                }
            }

            return constructor.Invoke(args);
        }

        private static object HandleGroupByTerminal(List<object> results, QueryTerminalKind terminal, Type resultType)
        {
            switch (terminal)
            {
                case QueryTerminalKind.First:
                    if (results.Count == 0)
                        throw new InvalidOperationException("Sequence contains no elements");
                    return results[0];
                case QueryTerminalKind.FirstOrDefault:
                    return results.Count > 0 ? results[0] : null;
                case QueryTerminalKind.Single:
                    if (results.Count == 0)
                        throw new InvalidOperationException("Sequence contains no elements");
                    if (results.Count > 1)
                        throw new InvalidOperationException("Sequence contains more than one element");
                    return results[0];
                case QueryTerminalKind.SingleOrDefault:
                    if (results.Count > 1)
                        throw new InvalidOperationException("Sequence contains more than one element");
                    return results.Count > 0 ? results[0] : null;
                case QueryTerminalKind.Sequence:
                default:
                    if (resultType.IsGenericType)
                    {
                        var elementType = resultType.GetGenericArguments()[0];
                        var listType = typeof(List<>).MakeGenericType(elementType);
                        var typedList = (IList)Activator.CreateInstance(listType);
                        foreach (var item in results)
                        {
                            typedList.Add(item);
                        }
                        return typedList;
                    }
                    return results;
            }
        }

        private static object ReadObject(IDataReader reader, TableMapping table, int startOrdinal)
        {
            var obj = Activator.CreateInstance(table.MappedType);
            for (var i = 0; i < table.Columns.Count; i++)
            {
                var col = table.Columns[i];
                var val = reader.GetValue(startOrdinal + i);
                if (val is DBNull)
                {
                    val = null;
                }
                if (val != null)
                {
                    col.SetValue(obj, val);
                }
            }
            return obj;
        }

        private static object HandleJoinTerminal(List<object> results, QueryTerminalKind terminal, Type resultType)
        {
            switch (terminal)
            {
                case QueryTerminalKind.First:
                    if (results.Count == 0)
                        throw new InvalidOperationException("Sequence contains no elements");
                    return results[0];
                case QueryTerminalKind.FirstOrDefault:
                    return results.Count > 0 ? results[0] : null;
                case QueryTerminalKind.Last:
                    if (results.Count == 0)
                        throw new InvalidOperationException("Sequence contains no elements");
                    return results[results.Count - 1];
                case QueryTerminalKind.LastOrDefault:
                    return results.Count > 0 ? results[results.Count - 1] : null;
                case QueryTerminalKind.Single:
                    if (results.Count == 0)
                        throw new InvalidOperationException("Sequence contains no elements");
                    if (results.Count > 1)
                        throw new InvalidOperationException("Sequence contains more than one element");
                    return results[0];
                case QueryTerminalKind.SingleOrDefault:
                    if (results.Count > 1)
                        throw new InvalidOperationException("Sequence contains more than one element");
                    return results.Count > 0 ? results[0] : null;
                case QueryTerminalKind.Sequence:
                default:
                    if (resultType.IsGenericType)
                    {
                        var elementType = resultType.GetGenericArguments()[0];
                        var listType = typeof(List<>).MakeGenericType(elementType);
                        var typedList = (IList)Activator.CreateInstance(listType);
                        foreach (var item in results)
                        {
                            typedList.Add(item);
                        }
                        return typedList;
                    }
                    return results;
            }
        }

        private static object ExecuteSequence(IDbCommand command, TableMapping table, LambdaExpression projection, Type resultType)
        {
            var method = typeof(SqlHelper)
                .GetMethod(nameof(SqlHelper.ExecuteQuery), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(table.MappedType);
            var result = method.Invoke(null, new object[] { command, table });

            if (projection != null)
            {
                return ApplyProjectionToSequence((IEnumerable)result, projection, resultType);
            }

            if (resultType.IsAssignableFrom(result.GetType()))
            {
                return result;
            }

            if (resultType.IsGenericType && resultType.GetGenericTypeDefinition() == typeof(System.Collections.Generic.IEnumerable<>))
            {
                return result;
            }

            return ((IEnumerable)result).Cast<object>().ToList();
        }

        private static async Task<object> ExecuteSequenceAsync(IDbCommand command, TableMapping table, LambdaExpression projection, Type resultType, CancellationToken cancellationToken)
        {
            var method = typeof(SqlHelper)
                .GetMethod("ExecuteQueryAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(table.MappedType);
            var task = (Task)method.Invoke(null, new object[] { command, table, cancellationToken });
            await task.ConfigureAwait(false);
            var result = task.GetType().GetProperty("Result").GetValue(task);

            if (projection != null)
            {
                return ApplyProjectionToSequence((IEnumerable)result, projection, resultType);
            }

            if (resultType.IsAssignableFrom(result.GetType()))
            {
                return result;
            }

            if (resultType.IsGenericType && resultType.GetGenericTypeDefinition() == typeof(System.Collections.Generic.IEnumerable<>))
            {
                return result;
            }

            return ((IEnumerable)result).Cast<object>().ToList();
        }

        private static int ExecuteCount(IDbCommand command)
        {
            var result = command.ExecuteScalar();
            if (result is long l)
            {
                return (int)l;
            }

            return (int)result;
        }

        private static long ExecuteLongCount(IDbCommand command)
        {
            var result = command.ExecuteScalar();
            if (result is int i)
            {
                return i;
            }

            return (long)result;
        }

        private static async Task<int> ExecuteCountAsync(IDbCommand command, CancellationToken cancellationToken)
        {
            var result = await command.TryExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (result is long l)
            {
                return (int)l;
            }

            return (int)result;
        }

        private static async Task<long> ExecuteLongCountAsync(IDbCommand command, CancellationToken cancellationToken)
        {
            var result = await command.TryExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (result is int i)
            {
                return i;
            }

            return (long)result;
        }

        private static object ExecuteFirst(IDbCommand command, TableMapping table, LambdaExpression projection, bool throwIfEmpty)
        {
            var method = typeof(SqlHelper)
                .GetMethod(nameof(SqlHelper.ExecuteQuery), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(table.MappedType);
            var list = (IEnumerable)method.Invoke(null, new object[] { command, table });
            var first = list.Cast<object>().FirstOrDefault();
            if (first == null && throwIfEmpty)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            if (first != null && projection != null)
            {
                var compiled = projection.Compile();
                return compiled.DynamicInvoke(first);
            }

            return first;
        }

        private static async Task<object> ExecuteFirstAsync(IDbCommand command, TableMapping table, LambdaExpression projection, bool throwIfEmpty, CancellationToken cancellationToken)
        {
            var method = typeof(SqlHelper)
                .GetMethod("ExecuteQueryAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(table.MappedType);
            var task = (Task)method.Invoke(null, new object[] { command, table, cancellationToken });
            await task.ConfigureAwait(false);
            var list = (IEnumerable)task.GetType().GetProperty("Result").GetValue(task);
            var first = list.Cast<object>().FirstOrDefault();
            if (first == null && throwIfEmpty)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            if (first != null && projection != null)
            {
                var compiled = projection.Compile();
                return compiled.DynamicInvoke(first);
            }

            return first;
        }

        private static object ApplyProjectionToSequence(IEnumerable source, LambdaExpression projection, Type resultType)
        {
            var compiled = projection.Compile();
            var projected = new List<object>();
            foreach (var item in source)
            {
                projected.Add(compiled.DynamicInvoke(item));
            }

            var elementType = resultType;
            if (resultType.IsGenericType)
            {
                var genDef = resultType.GetGenericTypeDefinition();
                if (genDef == typeof(IEnumerable<>) || genDef == typeof(List<>))
                {
                    elementType = resultType.GetGenericArguments()[0];
                }
            }

            var listType = typeof(List<>).MakeGenericType(elementType);
            var typedList = (IList)Activator.CreateInstance(listType);
            foreach (var item in projected)
            {
                typedList.Add(item);
            }

            return typedList;
        }

        private static object ExecuteSingle(IDbCommand command, TableMapping table, LambdaExpression projection, bool throwIfEmpty)
        {
            var method = typeof(SqlHelper)
                .GetMethod(nameof(SqlHelper.ExecuteQuery), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(table.MappedType);
            var list = ((IEnumerable)method.Invoke(null, new object[] { command, table })).Cast<object>().ToList();

            if (list.Count > 1)
            {
                throw new InvalidOperationException("Sequence contains more than one element");
            }

            var item = list.Count == 1 ? list[0] : null;
            if (item == null && throwIfEmpty)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            if (item != null && projection != null)
            {
                var compiled = projection.Compile();
                return compiled.DynamicInvoke(item);
            }

            return item;
        }

        private static async Task<object> ExecuteSingleAsync(IDbCommand command, TableMapping table, LambdaExpression projection, bool throwIfEmpty, CancellationToken cancellationToken)
        {
            var method = typeof(SqlHelper)
                .GetMethod("ExecuteQueryAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(table.MappedType);
            var task = (Task)method.Invoke(null, new object[] { command, table, cancellationToken });
            await task.ConfigureAwait(false);
            var list = ((IEnumerable)task.GetType().GetProperty("Result").GetValue(task)).Cast<object>().ToList();

            if (list.Count > 1)
            {
                throw new InvalidOperationException("Sequence contains more than one element");
            }

            var item = list.Count == 1 ? list[0] : null;
            if (item == null && throwIfEmpty)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            if (item != null && projection != null)
            {
                var compiled = projection.Compile();
                return compiled.DynamicInvoke(item);
            }

            return item;
        }

        private static object ExecuteAggregate(IDbCommand command, Type resultType)
        {
            var result = command.ExecuteScalar();
            return ConvertScalar(result, resultType);
        }

        private static async Task<object> ExecuteAggregateAsync(IDbCommand command, Type resultType, CancellationToken cancellationToken)
        {
            var result = await command.TryExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return ConvertScalar(result, resultType);
        }

        private static object ConvertScalar(object result, Type resultType)
        {
            if (result is null || result is DBNull)
            {
                if (resultType.IsValueType)
                {
                    return Activator.CreateInstance(resultType);
                }
                return null;
            }

            var targetType = resultType;
            if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                targetType = targetType.GetGenericArguments()[0];
            }

            return Convert.ChangeType(result, targetType);
        }
    }
}
