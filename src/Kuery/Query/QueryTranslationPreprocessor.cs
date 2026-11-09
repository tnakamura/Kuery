using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query
{
    internal sealed class QueryTranslationPreprocessor
    {
        internal Expression Process(Expression query)
        {
            return new InvocationExpressionRemovingExpressionVisitor().Visit(query);
        }

        private sealed class InvocationExpressionRemovingExpressionVisitor : ExpressionVisitor
        {
            protected override Expression VisitExtension(Expression node) => node;

            protected override Expression VisitInvocation(InvocationExpression node)
            {
                var expression = RelationalQueryableMethodTranslatingExpressionVisitor.StripQuote(node.Expression);
                if (expression is LambdaExpression lambda)
                    return Visit(RelationalQueryableMethodTranslatingExpressionVisitor.Bind(lambda, node.Arguments.ToArray()));
                throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(node);
            }
        }
    }
}
