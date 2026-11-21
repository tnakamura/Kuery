using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query
{
    internal sealed class QueryTranslationPreprocessor
    {
        internal Expression Process(Expression query)
        {
            return new QueryableMethodNormalizingExpressionVisitor().Visit(query);
        }

        private sealed class QueryableMethodNormalizingExpressionVisitor : ExpressionVisitor
        {
            protected override Expression VisitExtension(Expression node) => node;

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                var visited = (MethodCallExpression)base.VisitMethodCall(node);
                if (visited.Method.DeclaringType != typeof(Queryable)
                    || (visited.Method.Name != "Order" && visited.Method.Name != "OrderDescending"))
                    return visited;

                if (visited.Arguments.Count != 1) throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(visited);
                var elementType = visited.Method.GetGenericArguments()[0];
                var element = Expression.Parameter(elementType, "element");
                return Expression.Call(typeof(Queryable),
                    visited.Method.Name == "Order" ? nameof(Queryable.OrderBy) : nameof(Queryable.OrderByDescending),
                    new[] { elementType, elementType }, visited.Arguments[0], Expression.Quote(Expression.Lambda(element, element)));
            }

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
