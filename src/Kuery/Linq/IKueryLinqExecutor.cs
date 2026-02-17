using System;
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

        object ExecuteTerminal(KueryQueryContext context, Expression expression);

        Task<object> ExecuteTerminalAsync(KueryQueryContext context, Expression expression, CancellationToken cancellationToken);
    }
}
