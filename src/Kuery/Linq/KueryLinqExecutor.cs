using System;
using System.Linq.Expressions;

namespace Kuery.Linq
{
    internal sealed class KueryLinqExecutor : IKueryLinqExecutor
    {
        readonly QueryableModelTranslator _translator = new QueryableModelTranslator();

        public object BuildQueryModel(KueryQueryContext context, Expression expression)
        {
            Requires.NotNull(context, nameof(context));
            Requires.NotNull(expression, nameof(expression));
            return _translator.Translate(expression);
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
