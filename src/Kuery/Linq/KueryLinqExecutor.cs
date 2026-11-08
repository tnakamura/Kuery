using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kuery.Linq
{
    internal sealed class KueryLinqExecutor : IKueryLinqExecutor
    {
        internal RelationalCommand Prepare(KueryQueryContext context, Expression expression, Type resultType, bool async,
            out RelationalQueryPlan plan, string operation = "select", IReadOnlyList<SetPropertyCall> setters = null)
        {
            var normalizer = new QueryNormalizer(context.Connection);
            var normalized = normalizer.Visit(expression);
            var dialect = SqlDialectFactory.Create(context.Connection);
            var key = QueryPlanCache.Key(normalized, dialect.Kind, resultType, operation, async);
            List<SetPropertyCall> normalizedSetters = null;
            if (setters != null)
            {
                normalizedSetters = new List<SetPropertyCall>();
                foreach (var setter in setters)
                {
                    var property = (LambdaExpression)normalizer.Visit(setter.PropertyExpression);
                    var value = (LambdaExpression)normalizer.Visit(setter.ValueExpression);
                    normalizedSetters.Add(new SetPropertyCall(property, value));
                    key += QueryPlanCache.Key(property, dialect.Kind, resultType, "property", async)
                        + QueryPlanCache.Key(value, dialect.Kind, resultType, "value", async);
                }
            }
            plan = QueryPlanCache.GetOrAdd(key, () => operation == "select"
                ? new RelationalQueryCompiler().Compile(normalized)
                : new RelationalQueryCompiler().CompileMutation(normalized, operation, normalizedSetters));
            return RelationalSqlGenerator.GenerateCached(plan, dialect, normalizer.Values);
        }

        public object Execute(KueryQueryContext context, Expression expression, Type resultType)
        {
            var generated = Prepare(context, expression, resultType, false, out var plan);
            using (var command = CreateCommand(context.Connection, generated))
            using (var reader = command.ExecuteReader()) return Read(reader, plan);
        }

        public async Task<object> ExecuteAsync(KueryQueryContext context, Expression expression, Type resultType, CancellationToken cancellationToken)
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

        static IList CreateList(RelationalQueryPlan plan) => (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(plan.ElementType));
        static bool Finished(RelationalQueryPlan plan, int count) => plan.ResultKind != QueryResultKind.Sequence
            && (plan.ResultKind == QueryResultKind.Single || plan.ResultKind == QueryResultKind.SingleOrDefault ? count == 2 : count == 1);

        static object Read(IDataReader reader, RelationalQueryPlan plan, CancellationToken cancellationToken = default)
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

        static object Result(RelationalQueryPlan plan, IList rows)
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

        public int ExecuteDelete(KueryQueryContext context, Expression expression) => Mutate(context, expression, "delete", null);
        public Task<int> ExecuteDeleteAsync(KueryQueryContext context, Expression expression, CancellationToken cancellationToken)
            => MutateAsync(context, expression, "delete", null, cancellationToken);
        public int ExecuteUpdate(KueryQueryContext context, Expression expression, IReadOnlyList<SetPropertyCall> setters) => Mutate(context, expression, "update", setters);
        public Task<int> ExecuteUpdateAsync(KueryQueryContext context, Expression expression, IReadOnlyList<SetPropertyCall> setters, CancellationToken cancellationToken)
            => MutateAsync(context, expression, "update", setters, cancellationToken);

        int Mutate(KueryQueryContext context, Expression expression, string operation, IReadOnlyList<SetPropertyCall> setters)
        {
            var generated = Prepare(context, expression, typeof(int), false, out _, operation, setters);
            using (var command = CreateCommand(context.Connection, generated)) return command.ExecuteNonQuery();
        }

        async Task<int> MutateAsync(KueryQueryContext context, Expression expression, string operation, IReadOnlyList<SetPropertyCall> setters, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var generated = Prepare(context, expression, typeof(int), true, out _, operation, setters);
            using (var command = CreateCommand(context.Connection, generated)) return await command.TryExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

    }
}
