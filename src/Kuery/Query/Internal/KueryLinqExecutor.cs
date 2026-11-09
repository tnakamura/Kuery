using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Kuery.Query;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    internal sealed class KueryLinqExecutor : IKueryLinqExecutor
    {
        readonly QueryCompiler _queryCompiler = new QueryCompiler();

        internal RelationalCommand Prepare(QueryContext context, Expression expression, Type resultType, bool async,
            out CompiledQuery plan, string operation = "select", IReadOnlyList<SetPropertyCall> setters = null)
            => _queryCompiler.CompileQuery(context, expression, resultType, async, out plan, operation, setters);

        public object Execute(QueryContext context, Expression expression, Type resultType)
        {
            var generated = Prepare(context, expression, resultType, false, out var plan);
            using (var command = CreateCommand(context.Connection, generated))
            using (var reader = command.ExecuteReader()) return Read(reader, plan);
        }

        public async Task<object> ExecuteAsync(QueryContext context, Expression expression, Type resultType, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var generated = Prepare(context, expression, resultType, true, out var plan);
            using (var command = CreateCommand(context.Connection, generated))
            {
                if (!(command is DbCommand dbCommand))
                {
                    using (var reader = command.ExecuteReader()) return Read(reader, plan, cancellationToken);
                }
                using (var reader = await dbCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    var rows = CreateList(plan);
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        rows.Add(plan.Materialize(reader));
                        if (Finished(plan, rows.Count)) break;
                    }
                    return Result(plan, rows);
                }
            }
        }

        static IList CreateList(CompiledQuery plan) => (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(plan.ElementType));
        static bool Finished(CompiledQuery plan, int count) => plan.ResultKind != QueryResultKind.Sequence
            && (plan.ResultKind == QueryResultKind.Single || plan.ResultKind == QueryResultKind.SingleOrDefault ? count == 2 : count == 1);

        static object Read(IDataReader reader, CompiledQuery plan, CancellationToken cancellationToken = default)
        {
            var rows = CreateList(plan);
            while (reader.Read())
            {
                cancellationToken.ThrowIfCancellationRequested();
                rows.Add(plan.Materialize(reader));
                if (Finished(plan, rows.Count)) break;
            }
            return Result(plan, rows);
        }

        static object Result(CompiledQuery plan, IList rows)
        {
            if (plan.ResultKind == QueryResultKind.Sequence) return rows;
            if ((plan.ResultKind == QueryResultKind.Single || plan.ResultKind == QueryResultKind.SingleOrDefault) && rows.Count > 1)
                throw new InvalidOperationException("Sequence contains more than one element");
            if (rows.Count != 0) return rows[0];
            if (plan.ResultKind == QueryResultKind.FirstOrDefault || plan.ResultKind == QueryResultKind.SingleOrDefault || plan.ResultKind == QueryResultKind.ElementAtOrDefault)
                return plan.ElementType.IsValueType ? Activator.CreateInstance(plan.ElementType) : null;
            if (plan.ResultKind == QueryResultKind.ElementAt) throw new ArgumentOutOfRangeException("index");
            throw new InvalidOperationException("Sequence contains no elements");
        }

        static IDbCommand CreateCommand(IDbConnection connection, RelationalCommand generated)
        {
            var command = connection.CreateCommand();
            try
            {
                command.CommandText = generated.CommandText;
                foreach (var bound in generated.Parameters)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = bound.Name;
                    parameter.Value = bound.Value ?? DBNull.Value;
                    var type = Nullable.GetUnderlyingType(bound.Type) ?? bound.Type;
                    if (type == typeof(int)) parameter.DbType = DbType.Int32;
                    else if (type == typeof(long)) parameter.DbType = DbType.Int64;
                    else if (type == typeof(short)) parameter.DbType = DbType.Int16;
                    else if (type == typeof(bool)) parameter.DbType = DbType.Boolean;
                    else if (type == typeof(string)) parameter.DbType = DbType.String;
                    else if (type == typeof(double)) parameter.DbType = DbType.Double;
                    else if (type == typeof(float)) parameter.DbType = DbType.Single;
                    else if (type == typeof(decimal)) parameter.DbType = DbType.Decimal;
                    else if (type == typeof(Guid)) parameter.DbType = DbType.Guid;
                    else if (type == typeof(DateTime)) parameter.DbType = DbType.DateTime;
                    else if (type == typeof(DateTimeOffset)) parameter.DbType = DbType.DateTimeOffset;
                    command.Parameters.Add(parameter);
                }
                return command;
            }
            catch { command.Dispose(); throw; }
        }

        public int ExecuteDelete(QueryContext context, Expression expression) => Mutate(context, expression, "delete", null);
        public Task<int> ExecuteDeleteAsync(QueryContext context, Expression expression, CancellationToken cancellationToken)
            => MutateAsync(context, expression, "delete", null, cancellationToken);
        public int ExecuteUpdate(QueryContext context, Expression expression, IReadOnlyList<SetPropertyCall> setters) => Mutate(context, expression, "update", setters);
        public Task<int> ExecuteUpdateAsync(QueryContext context, Expression expression, IReadOnlyList<SetPropertyCall> setters, CancellationToken cancellationToken)
            => MutateAsync(context, expression, "update", setters, cancellationToken);

        int Mutate(QueryContext context, Expression expression, string operation, IReadOnlyList<SetPropertyCall> setters)
        {
            var generated = Prepare(context, expression, typeof(int), false, out _, operation, setters);
            using (var command = CreateCommand(context.Connection, generated)) return command.ExecuteNonQuery();
        }

        async Task<int> MutateAsync(QueryContext context, Expression expression, string operation, IReadOnlyList<SetPropertyCall> setters, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var generated = Prepare(context, expression, typeof(int), true, out _, operation, setters);
            using (var command = CreateCommand(context.Connection, generated)) return await command.TryExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

    }
}
