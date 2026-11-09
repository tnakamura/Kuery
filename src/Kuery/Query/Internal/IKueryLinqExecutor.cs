using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Kuery.Query;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query.Internal
{
    internal interface IKueryLinqExecutor
    {
        object Execute(QueryContext context, Expression expression, Type resultType);

        Task<object> ExecuteAsync(QueryContext context, Expression expression, Type resultType, CancellationToken cancellationToken);

        int ExecuteDelete(QueryContext context, Expression expression);

        Task<int> ExecuteDeleteAsync(QueryContext context, Expression expression, CancellationToken cancellationToken);

        int ExecuteUpdate(QueryContext context, Expression expression, IReadOnlyList<SetPropertyCall> setters);

        Task<int> ExecuteUpdateAsync(QueryContext context, Expression expression, IReadOnlyList<SetPropertyCall> setters, CancellationToken cancellationToken);
    }
}
