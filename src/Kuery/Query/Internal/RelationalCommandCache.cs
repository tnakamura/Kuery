using System.Linq.Expressions;
using System.Collections.Generic;
using System.Threading;

namespace Kuery.Query.Internal
{
    /// <summary>Reuses SQL command templates when their text and parameter layout are independent of runtime values.</summary>
    internal static class RelationalCommandCache
    {
        /// <summary>Binds a cached template or generates and caches SQL when its shape is stable.</summary>
        /// <param name="plan">The compiled plan that supplies SQL or row materialization delegates.</param>
        /// <param name="dialect">The provider SQL dialect for the operation.</param>
        /// <param name="values">The extracted runtime parameter values in slot order.</param>
        /// <returns>The command bound to the supplied runtime values.</returns>
        internal static RelationalCommand GetRelationalCommand(CompiledQuery plan, ISqlDialect dialect, IReadOnlyList<object> values)
        {
            var template = Volatile.Read(ref plan.CommandTemplate);
            if (template != null)
            {
                var bound = new RelationalCommand { CommandText = template.CommandText };
                foreach (var parameter in template.Parameters)
                    bound.Parameters.Add(new BoundParameter { Name = parameter.Name, Type = parameter.Type,
                        Value = BoundParameter.ConvertValue(values[parameter.SourceIndex], parameter.InputType) });
                return bound;
            }
            var generator = new QuerySqlGenerator(dialect, values);
            var command = generator.GetCommand(plan.NonQuery == null ? (Expression)plan.Query : plan.NonQuery);
            if (generator.CanCache)
            {
                template = new RelationalCommand { CommandText = command.CommandText };
                foreach (var parameter in command.Parameters)
                    template.Parameters.Add(new BoundParameter { Name = parameter.Name, Type = parameter.Type,
                        InputType = parameter.InputType, SourceIndex = parameter.SourceIndex });
                // Templates contain only slot metadata, never the first execution's values.
                Interlocked.CompareExchange(ref plan.CommandTemplate, template, null);
            }
            return command;
        }
    }
}
