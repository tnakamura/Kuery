using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kuery.Linq
{
    internal interface IKueryLinqExecutor
    {
        object BuildQueryModel(KueryQueryContext context, Expression expression);

        object Execute(KueryQueryContext context, Expression expression, Type resultType);

        Task<object> ExecuteAsync(KueryQueryContext context, Expression expression, Type resultType, CancellationToken cancellationToken);

        int ExecuteDelete(KueryQueryContext context, Expression expression);

        Task<int> ExecuteDeleteAsync(KueryQueryContext context, Expression expression, CancellationToken cancellationToken);

        int ExecuteUpdate(KueryQueryContext context, Expression expression, IReadOnlyList<SetPropertyCall> setters);

        Task<int> ExecuteUpdateAsync(KueryQueryContext context, Expression expression, IReadOnlyList<SetPropertyCall> setters, CancellationToken cancellationToken);

        object ExecuteTerminal(KueryQueryContext context, Expression expression);

        Task<object> ExecuteTerminalAsync(KueryQueryContext context, Expression expression, CancellationToken cancellationToken);
    }
}
