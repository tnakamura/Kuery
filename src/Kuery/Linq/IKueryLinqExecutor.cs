using System;
using System.Linq.Expressions;

namespace Kuery.Linq
{
    internal interface IKueryLinqExecutor
    {
        object BuildQueryModel(KueryQueryContext context, Expression expression);

        object Execute(KueryQueryContext context, Expression expression, Type resultType);

        object ExecuteTerminal(KueryQueryContext context, Expression expression);
    }
}
