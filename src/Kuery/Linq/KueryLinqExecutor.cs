using System;
using System.Linq.Expressions;

namespace Kuery.Linq
{
    internal sealed class KueryLinqExecutor : IKueryLinqExecutor
    {
        public object BuildQueryModel(KueryQueryContext context, Expression expression)
        {
            throw new NotSupportedException("LINQ query model builder is not implemented yet.");
        }

        public object Execute(KueryQueryContext context, Expression expression, Type resultType)
        {
            throw new NotSupportedException("LINQ SQL execution is not implemented yet.");
        }

        public object ExecuteTerminal(KueryQueryContext context, Expression expression)
        {
            throw new NotSupportedException("LINQ terminal execution is not implemented yet.");
        }
    }
}
