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
